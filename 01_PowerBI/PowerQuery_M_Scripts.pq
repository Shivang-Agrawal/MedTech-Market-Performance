// ==========================================================================
// MedTech Market Performance - Power Query (M) scripts
// Identical to the partitions/expressions in the PBIP semantic model (TMDL).
// Order: parameters -> staging queries (load disabled) -> loaded tables.
// ==========================================================================

// ---- PARAMETER: SourceFilePath  (Full local path to MedTech_Case_Study_Data.xlsx. Change via Transform data > Edit parameters.)
SourceFilePath = "C:\MedTechCaseStudy\MedTech_Case_Study_Data.xlsx" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true];

// ---- PARAMETER: OurCompanyName  (Manufacturer value that identifies Company Sales.)
OurCompanyName = "Our Company" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true];

// ---- PARAMETER: CompletenessVolumeThreshold  (A month is complete only if delivered rows >= this ratio of the trailing 3-month average.)
CompletenessVolumeThreshold = 0.8 meta [IsParameterQuery = true, Type = "Number", IsParameterQueryRequired = true];

// ---- PARAMETER: CompletenessCoverageToleranceDays  (A month is complete only if the last transaction date is within this many days of month end.)
CompletenessCoverageToleranceDays = 3 meta [IsParameterQuery = true, Type = "Number", IsParameterQueryRequired = true];

// ---- STAGING QUERY (Enable load = off): src_Workbook
src_Workbook =
let
    // Single connection to the supplied workbook. delayTypes = true so every column is typed explicitly downstream.
    Source = Excel.Workbook(File.Contents(SourceFilePath), null, true)
in
    Source;

// ---- STAGING QUERY (Enable load = off): stg_VendorRaw
stg_VendorRaw =
let
    Source = src_Workbook,
    SheetData = Source{[Item = "Vendor_Market_Data", Kind = "Sheet"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(SheetData, [PromoteAllScalars = true]),
    // ---- Schema validation: fail the refresh with a clear message if a required column is missing ----
    RequiredColumns = {"TransactionDate", "InvoiceMonth", "InvoiceNumber", "InvoiceLineNumber", "FacilityID", "FacilityName", "State", "Region", "ProductCode", "ProductDescription", "Manufacturer", "VendorBrand", "Units", "SalesAmount", "IngestedAt", "BatchID"},
    MissingColumns = List.Difference(RequiredColumns, Table.ColumnNames(PromotedHeaders)),
    SchemaValidated =
        if List.IsEmpty(MissingColumns)
        then Table.SelectColumns(PromotedHeaders, RequiredColumns)
        else error Error.Record("SchemaValidationFailed", "Vendor_Market_Data is missing required column(s): " & Text.Combine(MissingColumns, ", ")),
    RemovedBlankRows = Table.SelectRows(SchemaValidated, each [InvoiceNumber] <> null and [InvoiceNumber] <> ""),
    TypedColumns = Table.TransformColumnTypes(RemovedBlankRows, {
        {"TransactionDate", type date}, {"InvoiceMonth", type text}, {"InvoiceNumber", type text},
        {"InvoiceLineNumber", Int64.Type}, {"FacilityID", type text}, {"FacilityName", type text},
        {"State", type text}, {"Region", type text}, {"ProductCode", type text},
        {"ProductDescription", type text}, {"Manufacturer", type text}, {"VendorBrand", type text},
        {"Units", Int64.Type}, {"SalesAmount", Currency.Type}, {"IngestedAt", type datetime}, {"BatchID", type text}}),
    // Stable surrogate for lineage: position of the row in the source extract
    AddedRawRowId = Table.AddIndexColumn(TypedColumns, "RawRowId", 1, 1, Int64.Type),
    // ---- Product-code hygiene: keep the raw value for audit, derive a normalised join key ----
    RenamedRawCode = Table.RenameColumns(AddedRawRowId, {{"ProductCode", "ProductCodeRaw"}}),
    AddedCleanCode = Table.AddColumn(RenamedRawCode, "ProductCode", each Text.Upper(Text.Trim(Text.Clean([ProductCodeRaw]))), type text),
    AddedCodeNormalisedFlag = Table.AddColumn(AddedCleanCode, "CodeWasNormalised", each [ProductCodeRaw] <> [ProductCode], type logical),
    TrimmedText = Table.TransformColumns(AddedCodeNormalisedFlag, {
        {"InvoiceNumber", Text.Trim, type text}, {"FacilityID", each Text.Upper(Text.Trim(_)), type text},
        {"FacilityName", Text.Trim, type text}, {"State", each Text.Upper(Text.Trim(_)), type text},
        {"Region", Text.Trim, type text}, {"ProductDescription", Text.Trim, type text},
        {"Manufacturer", Text.Trim, type text}, {"VendorBrand", Text.Trim, type text}, {"BatchID", Text.Trim, type text}}),
    AddedMonthStart = Table.AddColumn(TrimmedText, "MonthStart", each Date.StartOfMonth([TransactionDate]), type date),
    // Consistency check: vendor InvoiceMonth must agree with TransactionDate
    AddedMonthCheck = Table.AddColumn(AddedMonthStart, "InvoiceMonthMatchesDate", each [InvoiceMonth] = Date.ToText([TransactionDate], [Format = "yyyy-MM", Culture = "en-US"]), type logical),
    Buffered = Table.Buffer(AddedMonthCheck)
in
    Buffered;

// ---- STAGING QUERY (Enable load = off): stg_VendorDedup
stg_VendorDedup =
let
    Source = stg_VendorRaw,
    // Natural key of an invoice line = InvoiceNumber + InvoiceLineNumber.
    // Keep the most recently ingested version; ties (exact re-sends) keep the first source row.
    GroupedByNaturalKey = Table.Group(Source, {"InvoiceNumber", "InvoiceLineNumber"}, {
        {"KeptRawRowId", each
            let
                LatestIngest = List.Max([IngestedAt])
            in
                List.Min(Table.SelectRows(_, (r) => r[IngestedAt] = LatestIngest)[RawRowId]), Int64.Type},
        {"KeyRowCount", each Table.RowCount(_), Int64.Type},
        {"DistinctVersions", each Table.RowCount(Table.Distinct(Table.SelectColumns(_, {"TransactionDate", "FacilityID", "ProductCode", "Manufacturer", "Units", "SalesAmount"}))), Int64.Type}}),
    MergedKeyInfo = Table.NestedJoin(Source, {"InvoiceNumber", "InvoiceLineNumber"}, GroupedByNaturalKey, {"InvoiceNumber", "InvoiceLineNumber"}, "KeyInfo", JoinKind.LeftOuter),
    ExpandedKeyInfo = Table.ExpandTableColumn(MergedKeyInfo, "KeyInfo", {"KeptRawRowId", "KeyRowCount", "DistinctVersions"}, {"KeptRawRowId", "KeyRowCount", "DistinctVersions"}),
    AddedIsDuplicate = Table.AddColumn(ExpandedKeyInfo, "IsDuplicate", each [RawRowId] <> [KeptRawRowId], type logical),
    AddedDuplicateType = Table.AddColumn(AddedIsDuplicate, "DuplicateType", each
        if not [IsDuplicate] then null
        else if [DistinctVersions] = 1 then "Exact duplicate"
        else "Superseded version", type text),
    SortedBySource = Table.Sort(AddedDuplicateType, {{"RawRowId", Order.Ascending}}),
    Buffered = Table.Buffer(SortedBySource)
in
    Buffered;

// ---- STAGING QUERY (Enable load = off): stg_ProductMapping
stg_ProductMapping =
let
    Source = src_Workbook,
    SheetData = Source{[Item = "Product_Scope", Kind = "Sheet"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(SheetData, [PromoteAllScalars = true]),
    RequiredColumns = {"ProductCode", "InternalBrand", "Category", "Subcategory", "ActiveFlag", "EffectiveFrom", "EffectiveTo"},
    MissingColumns = List.Difference(RequiredColumns, Table.ColumnNames(PromotedHeaders)),
    SchemaValidated =
        if List.IsEmpty(MissingColumns)
        then Table.SelectColumns(PromotedHeaders, RequiredColumns)
        else error Error.Record("SchemaValidationFailed", "Product_Scope is missing required column(s): " & Text.Combine(MissingColumns, ", ")),
    TypedColumns = Table.TransformColumnTypes(SchemaValidated, {
        {"ProductCode", type text}, {"InternalBrand", type text}, {"Category", type text}, {"Subcategory", type text},
        {"ActiveFlag", type text}, {"EffectiveFrom", type date}, {"EffectiveTo", type date}}),
    RemovedBlankCodes = Table.SelectRows(TypedColumns, each [ProductCode] <> null and Text.Trim([ProductCode]) <> ""),
    // Same normalisation as the vendor feed so the join key matches (" P015" -> "P015")
    Normalised = Table.TransformColumns(RemovedBlankCodes, {
        {"ProductCode", each Text.Upper(Text.Trim(Text.Clean(_))), type text},
        {"InternalBrand", Text.Trim, type text}, {"Category", Text.Trim, type text},
        {"Subcategory", Text.Trim, type text}, {"ActiveFlag", each Text.Upper(Text.Trim(_)), type text}}),
    Buffered = Table.Buffer(Normalised),
    // Raw counts (before de-duplication) - used to quantify join fan-out risk
    RawCounts = Table.Group(Buffered, {"ProductCode"}, {
        {"ScopeRowsRaw", each Table.RowCount(_), Int64.Type},
        {"ActiveRowsRaw", each Table.RowCount(Table.SelectRows(_, each [ActiveFlag] = "Y")), Int64.Type}}),
    // Rule 1: exact duplicate scope rows are collapsed (P012 and " P015" appear twice)
    DistinctRows = Table.Distinct(Buffered),
    ActiveRows = Table.SelectRows(DistinctRows, each [ActiveFlag] = "Y"),
    Grouped = Table.Group(DistinctRows, {"ProductCode"}, {
        {"ScopeRowsDistinct", each Table.RowCount(_), Int64.Type},
        {"ActiveMappings", each Table.RowCount(Table.Distinct(Table.SelectColumns(Table.SelectRows(_, each [ActiveFlag] = "Y"), {"InternalBrand", "Category", "Subcategory"}))), Int64.Type},
        {"InactiveRows", each Table.RowCount(Table.SelectRows(_, each [ActiveFlag] <> "Y")), Int64.Type},
        {"MappingDetail", each Text.Combine(List.Transform(Table.ToRecords(_), (r) =>
            (if r[ActiveFlag] = "Y" then "[Active] " else "[Inactive] ") & r[InternalBrand] & " / " & r[Category] & " / " & r[Subcategory]
            & " from " & (if r[EffectiveFrom] = null then "n/a" else Date.ToText(r[EffectiveFrom], [Format = "yyyy-MM-dd", Culture = "en-US"]))
            & (if r[EffectiveTo] = null then " (open)" else " to " & Date.ToText(r[EffectiveTo], [Format = "yyyy-MM-dd", Culture = "en-US"]))), " | "), type text}}),
    JoinedRawCounts = Table.NestedJoin(Grouped, {"ProductCode"}, RawCounts, {"ProductCode"}, "Raw", JoinKind.LeftOuter),
    ExpandedRawCounts = Table.ExpandTableColumn(JoinedRawCounts, "Raw", {"ScopeRowsRaw", "ActiveRowsRaw"}, {"ScopeRowsRaw", "ActiveRowsRaw"}),
    // Rule 2: exactly one distinct active mapping = Active; more than one = Ambiguous (quarantined); none = Inactive
    AddedStatus = Table.AddColumn(ExpandedRawCounts, "MappingStatus", each
        if [ActiveMappings] = 1 then "Active"
        else if [ActiveMappings] > 1 then "Ambiguous"
        else "Inactive", type text),
    AddedReason = Table.AddColumn(AddedStatus, "MappingReason", each
        if [MappingStatus] = "Active" then (if [ScopeRowsDistinct] < [ScopeRowsRaw] then "Single active mapping after collapsing exact duplicate scope rows" else "Single active mapping")
        else if [MappingStatus] = "Ambiguous" then Text.From([ActiveMappings]) & " conflicting active mappings with overlapping (open-ended) effective dates - quarantined pending data-steward decision"
        else "Flagged inactive (ActiveFlag = N) - out of scope", type text),
    ActiveMappingAttributes = Table.Group(ActiveRows, {"ProductCode", "InternalBrand", "Category", "Subcategory"}, {
        {"EffectiveFrom", each List.Min([EffectiveFrom]), type date},
        {"EffectiveTo", each if List.Contains([EffectiveTo], null) then null else List.Max([EffectiveTo]), type date}}),
    JoinedAttributes = Table.NestedJoin(AddedReason, {"ProductCode"}, ActiveMappingAttributes, {"ProductCode"}, "Attr", JoinKind.LeftOuter),
    AddedBrand = Table.AddColumn(JoinedAttributes, "Brand", each if [MappingStatus] = "Active" then [Attr]{0}[InternalBrand] else null, type text),
    AddedCategory = Table.AddColumn(AddedBrand, "Category", each if [MappingStatus] = "Active" then [Attr]{0}[Category] else null, type text),
    AddedSubcategory = Table.AddColumn(AddedCategory, "Subcategory", each if [MappingStatus] = "Active" then [Attr]{0}[Subcategory] else null, type text),
    AddedEffectiveFrom = Table.AddColumn(AddedSubcategory, "EffectiveFrom", each if [MappingStatus] = "Active" then [Attr]{0}[EffectiveFrom] else null, type date),
    AddedEffectiveTo = Table.AddColumn(AddedEffectiveFrom, "EffectiveTo", each if [MappingStatus] = "Active" then [Attr]{0}[EffectiveTo] else null, type date),
    RemovedAttr = Table.RemoveColumns(AddedEffectiveTo, {"Attr"}),
    Result = Table.Buffer(RemovedAttr)
in
    Result;

// ---- STAGING QUERY (Enable load = off): stg_CanonicalDescription
stg_CanonicalDescription =
let
    // Vendor sends alternative descriptions for the same code (e.g. "... - Alt").
    // Canonical description = most frequent description per code (ties: alphabetical).
    Source = Table.SelectRows(stg_VendorDedup, each not [IsDuplicate]),
    Counted = Table.Group(Source, {"ProductCode", "ProductDescription"}, {{"DescriptionRows", each Table.RowCount(_), Int64.Type}}),
    Sorted = Table.Buffer(Table.Sort(Counted, {{"ProductCode", Order.Ascending}, {"DescriptionRows", Order.Descending}, {"ProductDescription", Order.Ascending}})),
    FirstPerCode = Table.Distinct(Sorted, {"ProductCode"}),
    Renamed = Table.RenameColumns(FirstPerCode, {{"ProductDescription", "CanonicalDescription"}}),
    DescriptionCounts = Table.Group(Counted, {"ProductCode"}, {{"DistinctDescriptions", each Table.RowCount(_), Int64.Type}}),
    JoinedCounts = Table.NestedJoin(Renamed, {"ProductCode"}, DescriptionCounts, {"ProductCode"}, "C", JoinKind.LeftOuter),
    ExpandedCounts = Table.ExpandTableColumn(JoinedCounts, "C", {"DistinctDescriptions"}, {"DistinctDescriptions"}),
    // Vendor-reported manufacturer per code (most frequent) - used for DimProduct attribution
    ManufacturerCounted = Table.Group(Source, {"ProductCode", "Manufacturer"}, {{"ManufacturerRows", each Table.RowCount(_), Int64.Type}}),
    ManufacturerSorted = Table.Buffer(Table.Sort(ManufacturerCounted, {{"ProductCode", Order.Ascending}, {"ManufacturerRows", Order.Descending}, {"Manufacturer", Order.Ascending}})),
    ManufacturerFirst = Table.Distinct(ManufacturerSorted, {"ProductCode"}),
    JoinedManufacturer = Table.NestedJoin(ExpandedCounts, {"ProductCode"}, ManufacturerFirst, {"ProductCode"}, "M", JoinKind.LeftOuter),
    ExpandedManufacturer = Table.ExpandTableColumn(JoinedManufacturer, "M", {"Manufacturer"}, {"Manufacturer"}),
    Selected = Table.SelectColumns(ExpandedManufacturer, {"ProductCode", "CanonicalDescription", "DistinctDescriptions", "Manufacturer"}),
    Buffered = Table.Buffer(Selected)
in
    Buffered;

// ---- STAGING QUERY (Enable load = off): stg_VendorAudit
stg_VendorAudit =
let
    Source = stg_VendorDedup,
    JoinedMapping = Table.NestedJoin(Source, {"ProductCode"}, stg_ProductMapping, {"ProductCode"}, "Map", JoinKind.LeftOuter),
    ExpandedMapping = Table.ExpandTableColumn(JoinedMapping, "Map", {"MappingStatus", "EffectiveFrom", "EffectiveTo"}, {"MappingStatus", "MapEffectiveFrom", "MapEffectiveTo"}),
    // Status precedence: Duplicate > Unmapped > Inactive > Ambiguous > Outside effective window > Accepted
    AddedRowStatus = Table.AddColumn(ExpandedMapping, "RowStatus", each
        if [IsDuplicate] then "Rejected - Duplicate"
        else if [MappingStatus] = null then "Rejected - Unmapped product"
        else if [MappingStatus] = "Inactive" then "Rejected - Inactive product"
        else if [MappingStatus] = "Ambiguous" then "Rejected - Ambiguous mapping"
        else if ([MapEffectiveFrom] <> null and [TransactionDate] < [MapEffectiveFrom]) or ([MapEffectiveTo] <> null and [TransactionDate] > [MapEffectiveTo]) then "Rejected - Outside effective window"
        else "Accepted", type text),
    AddedRejectReason = Table.AddColumn(AddedRowStatus, "RejectReason", each
        if [RowStatus] = "Accepted" then null
        else if [RowStatus] = "Rejected - Duplicate" then [DuplicateType] & " of RawRowId " & Text.From([KeptRawRowId])
        else if [RowStatus] = "Rejected - Unmapped product" then "Code " & [ProductCode] & " not in Product_Scope"
        else if [RowStatus] = "Rejected - Inactive product" then "Code " & [ProductCode] & " has ActiveFlag = N"
        else if [RowStatus] = "Rejected - Ambiguous mapping" then "Code " & [ProductCode] & " has conflicting active mappings"
        else "Transaction date outside mapping effective window", type text),
    AddedStatusSort = Table.AddColumn(AddedRejectReason, "StatusSortOrder", each
        if [RowStatus] = "Accepted" then 1
        else if [RowStatus] = "Rejected - Duplicate" then 2
        else if [RowStatus] = "Rejected - Inactive product" then 3
        else if [RowStatus] = "Rejected - Unmapped product" then 4
        else if [RowStatus] = "Rejected - Ambiguous mapping" then 5
        else 6, Int64.Type),
    ReplacedNullStatus = Table.ReplaceValue(AddedStatusSort, null, "Unmapped", Replacer.ReplaceValue, {"MappingStatus"}),
    JoinedCanonical = Table.NestedJoin(ReplacedNullStatus, {"ProductCode"}, stg_CanonicalDescription, {"ProductCode"}, "Desc", JoinKind.LeftOuter),
    ExpandedCanonical = Table.ExpandTableColumn(JoinedCanonical, "Desc", {"CanonicalDescription"}, {"CanonicalDescription"}),
    AddedIsAltDescription = Table.AddColumn(ExpandedCanonical, "IsAltDescription", each [CanonicalDescription] <> null and [ProductDescription] <> [CanonicalDescription], type logical),
    SortedBySource = Table.Sort(AddedIsAltDescription, {{"RawRowId", Order.Ascending}}),
    Buffered = Table.Buffer(SortedBySource)
in
    Buffered;

// ---- LOADED TABLE: FactSales
// Accepted invoice lines only (grain: one vendor invoice line). De-duplicated, active, unambiguous product mapping.
FactSales =
let
    Source = stg_VendorAudit,
    // Only accepted rows reach the fact table: de-duplicated, active, unambiguous, in-window.
    Accepted = Table.SelectRows(Source, each [RowStatus] = "Accepted"),
    Selected = Table.SelectColumns(Accepted, {"RawRowId", "InvoiceNumber", "InvoiceLineNumber", "TransactionDate", "FacilityID", "ProductCode", "Manufacturer", "Units", "SalesAmount", "BatchID", "IngestedAt"}),
    Typed = Table.TransformColumnTypes(Selected, {
        {"RawRowId", Int64.Type}, {"InvoiceNumber", type text}, {"InvoiceLineNumber", Int64.Type},
        {"TransactionDate", type date}, {"FacilityID", type text}, {"ProductCode", type text},
        {"Manufacturer", type text}, {"Units", Int64.Type}, {"SalesAmount", Currency.Type},
        {"BatchID", type text}, {"IngestedAt", type datetime}})
in
    Typed;

// ---- LOADED TABLE: DimProduct
// In-scope, active, unambiguous products only. Hierarchy: Category > Subcategory > Brand > Product.
DimProduct =
let
    Source = Table.SelectRows(stg_ProductMapping, each [MappingStatus] = "Active"),
    JoinedDescription = Table.NestedJoin(Source, {"ProductCode"}, stg_CanonicalDescription, {"ProductCode"}, "Desc", JoinKind.LeftOuter),
    ExpandedDescription = Table.ExpandTableColumn(JoinedDescription, "Desc", {"CanonicalDescription", "DistinctDescriptions", "Manufacturer"}, {"ProductName", "VendorDescriptionCount", "Manufacturer"}),
    FilledName = Table.AddColumn(ExpandedDescription, "ProductNameFinal", each if [ProductName] = null then [ProductCode] else [ProductName], type text),
    AddedLabel = Table.AddColumn(FilledName, "Product", each [ProductCode] & " | " & [ProductNameFinal], type text),
    AddedIsCompany = Table.AddColumn(AddedLabel, "IsOurCompanyProduct", each [Manufacturer] = OurCompanyName, type logical),
    Selected = Table.SelectColumns(AddedIsCompany, {"ProductCode", "Product", "ProductNameFinal", "Brand", "Category", "Subcategory", "Manufacturer", "IsOurCompanyProduct", "EffectiveFrom", "EffectiveTo", "VendorDescriptionCount"}),
    Renamed = Table.RenameColumns(Selected, {{"ProductNameFinal", "ProductName"}}),
    Typed = Table.TransformColumnTypes(Renamed, {
        {"ProductCode", type text}, {"Product", type text}, {"ProductName", type text}, {"Brand", type text},
        {"Category", type text}, {"Subcategory", type text}, {"Manufacturer", type text},
        {"IsOurCompanyProduct", type logical}, {"EffectiveFrom", type date}, {"EffectiveTo", type date},
        {"VendorDescriptionCount", Int64.Type}}),
    Sorted = Table.Sort(Typed, {{"ProductCode", Order.Ascending}})
in
    Sorted;

// ---- LOADED TABLE: DimFacility
// Facility geography. RLS is applied on this table. Hierarchy: Region > State > Facility.
DimFacility =
let
    Source = Table.SelectRows(stg_VendorDedup, each not [IsDuplicate]),
    // Latest ingested attributes win if a facility is ever re-described by the vendor
    Sorted = Table.Sort(Source, {{"FacilityID", Order.Ascending}, {"IngestedAt", Order.Descending}}),
    Selected = Table.Buffer(Table.SelectColumns(Sorted, {"FacilityID", "FacilityName", "State", "Region"})),
    LatestPerFacility = Table.Distinct(Selected, {"FacilityID"}),
    AddedLabel = Table.AddColumn(LatestPerFacility, "Facility", each [FacilityID] & " | " & [FacilityName], type text),
    Typed = Table.TransformColumnTypes(AddedLabel, {{"FacilityID", type text}, {"FacilityName", type text}, {"State", type text}, {"Region", type text}, {"Facility", type text}})
in
    Typed;

// ---- LOADED TABLE: DimManufacturer
// Manufacturer lookup; IsOurCompany drives Company Sales.
DimManufacturer =
let
    Source = Table.SelectRows(stg_VendorDedup, each not [IsDuplicate]),
    Distinct = Table.Distinct(Table.SelectColumns(Source, {"Manufacturer"})),
    AddedIsOurCompany = Table.AddColumn(Distinct, "IsOurCompany", each [Manufacturer] = OurCompanyName, type logical),
    AddedType = Table.AddColumn(AddedIsOurCompany, "ManufacturerType", each if [IsOurCompany] then "Our Company" else "Competitor", type text),
    Sorted = Table.Sort(AddedType, {{"IsOurCompany", Order.Descending}, {"Manufacturer", Order.Ascending}}),
    AddedSort = Table.AddIndexColumn(Sorted, "ManufacturerSort", 1, 1, Int64.Type)
in
    AddedSort;

// ---- LOADED TABLE: DQ_PeriodCompleteness
// Monthly delivery completeness assessment (coverage + volume rules).
DQ_PeriodCompleteness =
let
    // Completeness is a property of the vendor delivery, so it is measured on all de-duplicated rows
    Source = Table.SelectRows(stg_VendorAudit, each not [IsDuplicate]),
    Grouped = Table.Group(Source, {"MonthStart"}, {
        {"DeliveredRows", each Table.RowCount(_), Int64.Type},
        {"AcceptedRows", each List.Count(List.Select([RowStatus], each _ = "Accepted")), Int64.Type},
        {"FirstTransactionDate", each List.Min([TransactionDate]), type date},
        {"LastTransactionDate", each List.Max([TransactionDate]), type date},
        {"DistinctTransactionDays", each List.Count(List.Distinct([TransactionDate])), Int64.Type},
        {"BatchCount", each List.Count(List.Distinct([BatchID])), Int64.Type}}),
    Sorted = Table.Buffer(Table.Sort(Grouped, {{"MonthStart", Order.Ascending}})),
    RowList = List.Buffer(Sorted[DeliveredRows]),
    Indexed = Table.AddIndexColumn(Sorted, "MonthIndex", 0, 1, Int64.Type),
    AddedTrailingAvg = Table.AddColumn(Indexed, "Trailing3MonthAvgRows", each
        if [MonthIndex] = 0 then null
        else List.Average(List.Range(RowList, List.Max({0, [MonthIndex] - 3}), List.Min({3, [MonthIndex]}))), type number),
    AddedVolumeRatio = Table.AddColumn(AddedTrailingAvg, "VolumeRatio", each if [Trailing3MonthAvgRows] = null then null else [DeliveredRows] / [Trailing3MonthAvgRows], type number),
    AddedMonthEnd = Table.AddColumn(AddedVolumeRatio, "MonthEnd", each Date.EndOfMonth([MonthStart]), type date),
    AddedDaysShort = Table.AddColumn(AddedMonthEnd, "DaysBeforeMonthEnd", each Duration.Days([MonthEnd] - [LastTransactionDate]), Int64.Type),
    AddedCoverageCheck = Table.AddColumn(AddedDaysShort, "CoverageCheckPassed", each [DaysBeforeMonthEnd] <= CompletenessCoverageToleranceDays, type logical),
    AddedVolumeCheck = Table.AddColumn(AddedCoverageCheck, "VolumeCheckPassed", each [VolumeRatio] = null or [VolumeRatio] >= CompletenessVolumeThreshold, type logical),
    AddedIsComplete = Table.AddColumn(AddedVolumeCheck, "IsComplete", each [CoverageCheckPassed] and [VolumeCheckPassed], type logical),
    AddedStatus = Table.AddColumn(AddedIsComplete, "PeriodStatus", each if [IsComplete] then "Complete" else "Incomplete", type text),
    AddedReason = Table.AddColumn(AddedStatus, "CompletenessReason", each
        if [IsComplete] then "Coverage and volume checks passed"
        else Text.Combine(List.RemoveNulls({
            if [CoverageCheckPassed] then null else "Data ends " & Date.ToText([LastTransactionDate], [Format = "dd-MMM-yyyy", Culture = "en-US"]) & " (" & Text.From([DaysBeforeMonthEnd]) & " days before month end)",
            if [VolumeCheckPassed] then null else "Volume " & Number.ToText([VolumeRatio], "P0", "en-US") & " of trailing 3-month average"}), "; "), type text),
    AddedMonthLabel = Table.AddColumn(AddedReason, "MonthLabel", each Date.ToText([MonthStart], [Format = "MMM yyyy", Culture = "en-US"]), type text),
    AddedYearMonthKey = Table.AddColumn(AddedMonthLabel, "YearMonthKey", each Date.Year([MonthStart]) * 100 + Date.Month([MonthStart]), Int64.Type),
    RemovedIndex = Table.RemoveColumns(AddedYearMonthKey, {"MonthIndex"})
in
    RemovedIndex;

// ---- LOADED TABLE: DimDate
// Marked date table (daily). Completeness flags come from DQ_PeriodCompleteness.
DimDate =
let
    Completeness = DQ_PeriodCompleteness,
    CompleteMonths = List.Buffer(Table.SelectRows(Completeness, each [IsComplete])[MonthStart]),
    LatestCompleteMonth = List.Max(CompleteMonths),
    StartDate = Date.StartOfYear(List.Min(stg_VendorRaw[TransactionDate])),
    EndDate = Date.EndOfYear(List.Max(stg_VendorRaw[TransactionDate])),
    DateList = List.Dates(StartDate, Duration.Days(EndDate - StartDate) + 1, #duration(1, 0, 0, 0)),
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), type table [Date = date]),
    AddedYear = Table.AddColumn(DateTable, "Year", each Date.Year([Date]), Int64.Type),
    AddedQuarter = Table.AddColumn(AddedYear, "Quarter", each "Q" & Text.From(Date.QuarterOfYear([Date])), type text),
    AddedMonthNumber = Table.AddColumn(AddedQuarter, "MonthNumber", each Date.Month([Date]), Int64.Type),
    AddedMonthName = Table.AddColumn(AddedMonthNumber, "MonthName", each Date.ToText([Date], [Format = "MMM", Culture = "en-US"]), type text),
    AddedMonthStart = Table.AddColumn(AddedMonthName, "MonthStart", each Date.StartOfMonth([Date]), type date),
    AddedYearMonthKey = Table.AddColumn(AddedMonthStart, "YearMonthKey", each Date.Year([Date]) * 100 + Date.Month([Date]), Int64.Type),
    AddedYearMonth = Table.AddColumn(AddedYearMonthKey, "YearMonth", each Date.ToText([Date], [Format = "yyyy-MM", Culture = "en-US"]), type text),
    AddedMonthYearLabel = Table.AddColumn(AddedYearMonth, "MonthYear", each Date.ToText([Date], [Format = "MMM yyyy", Culture = "en-US"]), type text),
    JoinedStatus = Table.NestedJoin(AddedMonthYearLabel, {"MonthStart"}, Table.SelectColumns(Completeness, {"MonthStart", "PeriodStatus"}), {"MonthStart"}, "C", JoinKind.LeftOuter),
    ExpandedStatus = Table.ExpandTableColumn(JoinedStatus, "C", {"PeriodStatus"}, {"PeriodStatusRaw"}),
    AddedPeriodStatus = Table.AddColumn(ExpandedStatus, "PeriodStatus", each if [PeriodStatusRaw] = null then "No Data" else [PeriodStatusRaw], type text),
    AddedIsComplete = Table.AddColumn(AddedPeriodStatus, "IsCompleteMonth", each [PeriodStatus] = "Complete", type logical),
    AddedIsLatest = Table.AddColumn(AddedIsComplete, "IsLatestCompleteMonth", each [MonthStart] = LatestCompleteMonth, type logical),
    AddedOffset = Table.AddColumn(AddedIsLatest, "MonthOffset", each (Date.Year([MonthStart]) - Date.Year(LatestCompleteMonth)) * 12 + Date.Month([MonthStart]) - Date.Month(LatestCompleteMonth), Int64.Type),
    // Like-for-like YoY: current month complete AND the same month last year complete
    AddedHasPY = Table.AddColumn(AddedOffset, "HasCompletePriorYearMonth", each List.Contains(CompleteMonths, Date.AddYears([MonthStart], -1)), type logical),
    AddedYoYComparable = Table.AddColumn(AddedHasPY, "IsYoYComparable", each [IsCompleteMonth] and [HasCompletePriorYearMonth], type logical),
    RemovedHelper = Table.RemoveColumns(AddedYoYComparable, {"PeriodStatusRaw"}),
    Sorted = Table.Sort(RemovedHelper, {{"Date", Order.Ascending}})
in
    Sorted;

// ---- LOADED TABLE: DQ_RowAudit
// Every raw vendor row with its data-quality outcome. Drives the Data Quality page. Secured via DimFacility.
DQ_RowAudit =
let
    Source = stg_VendorAudit,
    Selected = Table.SelectColumns(Source, {"RawRowId", "InvoiceNumber", "InvoiceLineNumber", "TransactionDate", "FacilityID", "ProductCodeRaw", "ProductCode", "CodeWasNormalised", "ProductDescription", "IsAltDescription", "Manufacturer", "Units", "SalesAmount", "BatchID", "IngestedAt", "InvoiceMonthMatchesDate", "IsDuplicate", "DuplicateType", "MappingStatus", "RowStatus", "RejectReason", "StatusSortOrder"}),
    Typed = Table.TransformColumnTypes(Selected, {
        {"RawRowId", Int64.Type}, {"InvoiceNumber", type text}, {"InvoiceLineNumber", Int64.Type}, {"TransactionDate", type date},
        {"FacilityID", type text}, {"ProductCodeRaw", type text}, {"ProductCode", type text}, {"CodeWasNormalised", type logical},
        {"ProductDescription", type text}, {"IsAltDescription", type logical}, {"Manufacturer", type text}, {"Units", Int64.Type},
        {"SalesAmount", Currency.Type}, {"BatchID", type text}, {"IngestedAt", type datetime}, {"InvoiceMonthMatchesDate", type logical},
        {"IsDuplicate", type logical}, {"DuplicateType", type text}, {"MappingStatus", type text}, {"RowStatus", type text},
        {"RejectReason", type text}, {"StatusSortOrder", Int64.Type}})
in
    Typed;

// ---- LOADED TABLE: DQ_ProductMapping
// One row per normalised product code seen in Product_Scope or the vendor feed, with the mapping decision.
DQ_ProductMapping =
let
    VendorRows = Table.SelectRows(stg_VendorAudit, each not [IsDuplicate]),
    VendorStats = Table.Group(VendorRows, {"ProductCode"}, {
        {"VendorRows", each Table.RowCount(_), Int64.Type},
        {"VendorSales", each List.Sum([SalesAmount]), Currency.Type},
        {"RawCodeVariants", each List.Count(List.Distinct([ProductCodeRaw])), Int64.Type},
        {"RowsWithCodeNormalised", each List.Count(List.Select([CodeWasNormalised], each _)), Int64.Type},
        {"DistinctDescriptions", each List.Count(List.Distinct([ProductDescription])), Int64.Type}}),
    RenamedVendorKey = Table.RenameColumns(VendorStats, {{"ProductCode", "VendorProductCode"}}),
    Joined = Table.NestedJoin(stg_ProductMapping, {"ProductCode"}, RenamedVendorKey, {"VendorProductCode"}, "V", JoinKind.FullOuter),
    Expanded = Table.ExpandTableColumn(Joined, "V", {"VendorProductCode", "VendorRows", "VendorSales", "RawCodeVariants", "RowsWithCodeNormalised", "DistinctDescriptions"}),
    AddedCode = Table.AddColumn(Expanded, "Code", each if [ProductCode] = null then [VendorProductCode] else [ProductCode], type text),
    AddedStatus = Table.AddColumn(AddedCode, "Status", each if [MappingStatus] = null then "Unmapped" else [MappingStatus], type text),
    AddedReason = Table.AddColumn(AddedStatus, "Reason", each if [MappingStatus] = null then "Code present in vendor feed but not in Product_Scope" else [MappingReason], type text),
    AddedAccepted = Table.AddColumn(AddedReason, "AcceptedInModel", each if [Status] = "Active" then "Yes" else "No", type text),
    // Sales that a naive merge on the raw scope table would have double-counted (join fan-out)
    AddedFanOut = Table.AddColumn(AddedAccepted, "NaiveJoinExtraSales", each
        if [ActiveRowsRaw] <> null and [ActiveRowsRaw] > 1 and [VendorSales] <> null then [VendorSales] * ([ActiveRowsRaw] - 1) else 0, Currency.Type),
    Selected = Table.SelectColumns(AddedFanOut, {"Code", "Status", "Reason", "AcceptedInModel", "ScopeRowsRaw", "ScopeRowsDistinct", "ActiveRowsRaw", "ActiveMappings", "MappingDetail", "VendorRows", "VendorSales", "RawCodeVariants", "RowsWithCodeNormalised", "DistinctDescriptions", "NaiveJoinExtraSales"}),
    RenamedCode = Table.RenameColumns(Selected, {{"Code", "ProductCode"}, {"Status", "MappingStatus"}, {"Reason", "MappingReason"}}),
    ReplacedNulls = Table.ReplaceValue(RenamedCode, null, 0, Replacer.ReplaceValue, {"ScopeRowsRaw", "ScopeRowsDistinct", "ActiveRowsRaw", "ActiveMappings", "VendorRows", "VendorSales", "RawCodeVariants", "RowsWithCodeNormalised", "DistinctDescriptions"}),
    Typed = Table.TransformColumnTypes(ReplacedNulls, {
        {"ProductCode", type text}, {"MappingStatus", type text}, {"MappingReason", type text}, {"AcceptedInModel", type text},
        {"ScopeRowsRaw", Int64.Type}, {"ScopeRowsDistinct", Int64.Type}, {"ActiveRowsRaw", Int64.Type}, {"ActiveMappings", Int64.Type},
        {"MappingDetail", type text}, {"VendorRows", Int64.Type}, {"VendorSales", Currency.Type}, {"RawCodeVariants", Int64.Type},
        {"RowsWithCodeNormalised", Int64.Type}, {"DistinctDescriptions", Int64.Type}, {"NaiveJoinExtraSales", Currency.Type}}),
    Sorted = Table.Sort(Typed, {{"ProductCode", Order.Ascending}})
in
    Sorted;

// ---- LOADED TABLE: UserAccess
// Facility_Access security mapping (de-duplicated, lower-case e-mail). Disconnected; used only by the RLS role.
UserAccess =
let
    Source = src_Workbook,
    SheetData = Source{[Item = "Facility_Access", Kind = "Sheet"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(SheetData, [PromoteAllScalars = true]),
    RequiredColumns = {"UserEmail", "Region", "State", "FacilityID", "AccessLevel"},
    MissingColumns = List.Difference(RequiredColumns, Table.ColumnNames(PromotedHeaders)),
    SchemaValidated =
        if List.IsEmpty(MissingColumns)
        then Table.SelectColumns(PromotedHeaders, RequiredColumns)
        else error Error.Record("SchemaValidationFailed", "Facility_Access is missing required column(s): " & Text.Combine(MissingColumns, ", ")),
    TypedColumns = Table.TransformColumnTypes(SchemaValidated, {{"UserEmail", type text}, {"Region", type text}, {"State", type text}, {"FacilityID", type text}, {"AccessLevel", type text}}),
    // UPN comparison is done in lower case; blank cells become null
    Normalised = Table.TransformColumns(TypedColumns, {
        {"UserEmail", each Text.Lower(Text.Trim(_)), type text}, {"Region", each if _ = null or Text.Trim(_) = "" then null else Text.Trim(_), type text},
        {"State", each if _ = null or Text.Trim(_) = "" then null else Text.Upper(Text.Trim(_)), type text},
        {"FacilityID", each if _ = null or Text.Trim(_) = "" then null else Text.Upper(Text.Trim(_)), type text},
        {"AccessLevel", each Text.Proper(Text.Trim(_)), type text}}),
    RemovedBlankUsers = Table.SelectRows(Normalised, each [UserEmail] <> null and [UserEmail] <> ""),
    // westlead@example.com is listed twice - collapse exact duplicates
    Deduplicated = Table.Distinct(RemovedBlankUsers)
in
    Deduplicated;

// ---- LOADED TABLE: ControlTotals
// Expected control totals used for reconciliation on the Data Quality page.
ControlTotals =
let
    Source = src_Workbook,
    ControlSheet = Table.SelectRows(Source, each [Item] = "_Control_Totals" and [Kind] = "Sheet"),
    // Fallback: independently recomputed control totals (documented in Submission Notes) if the sheet is absent
    Fallback = #table(type table [Metric = text, Value = number], {
        {"RawRows", 12055}, {"CleanRows", 10456}, {"DuplicateTransactionsRemoved", 55}, {"InactiveOrUnmappedRejected", 1146},
        {"AmbiguousMappingRejected", 398}, {"CleanSales", 17478530.67}, {"CleanUnits", 99465}}),
    Loaded = if Table.IsEmpty(ControlSheet) then Fallback else Table.PromoteHeaders(ControlSheet{0}[Data], [PromoteAllScalars = true]),
    SourceLabel = if Table.IsEmpty(ControlSheet) then "Independent recomputation (fallback)" else "Workbook sheet _Control_Totals",
    SelectedColumns = Table.SelectColumns(Loaded, {"Metric", "Value"}),
    Renamed = Table.RenameColumns(SelectedColumns, {{"Value", "ExpectedValue"}}),
    Typed = Table.TransformColumnTypes(Renamed, {{"Metric", type text}, {"ExpectedValue", type number}}),
    RemovedBlank = Table.SelectRows(Typed, each [Metric] <> null and [Metric] <> ""),
    AddedGroup = Table.AddColumn(RemovedBlank, "MetricGroup", each
        if Text.StartsWith([Metric], "CleanSales_") then "3. Monthly accepted sales"
        else if List.Contains({"CleanSales", "CleanUnits"}, [Metric]) then "2. Accepted totals"
        else "1. Row counts", type text),
    AddedSort = Table.AddIndexColumn(AddedGroup, "SortOrder", 1, 1, Int64.Type),
    AddedSource = Table.AddColumn(AddedSort, "ControlSource", each SourceLabel, type text)
in
    AddedSource;

// ---- LOADED TABLE: RefreshInfo
// Refresh audit (UTC timestamp captured by Power Query at refresh time).
RefreshInfo =
let
    Source = #table(type table [RefreshTimestampUTC = datetime, SourceFileName = text], {
        {DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), List.Last(Text.Split(SourceFilePath, "\"))}})
in
    Source;
