################################################################################
## Loading packages that are used.                                            ##
################################################################################
using CSV, DataFrames, DataFramesMeta
using Gadfly
using ZipFile
using FTPClient
#################################################################################

#################################################################################
## Loading data from CDPR FTP server.                                          ##
#################################################################################
"""
    download_info(year1[, year2])

Saves the zip files from CDPR server into your computer memory
"""
function download_files(year1::Int, year2=Union{Nothing,Int})
    ftp_init()
    options = RequestOptions(hostname="transfer.cdpr.ca.gov/pub/outgoing/pur_archives/")
    global files = DataFrame(year = [], files = [])

    if year2 != nothing
        first_year = minimum([year1, year2])
        second_year = maximum([year1, year2])
        years = collect(UnitRange(first_year, second_year))
    else
        years = [year1]
    end

    for i in 1:length(years)
        name = "pur" * string(years[i]) *".zip"
        resp = (year = years[i], files = ftp_get(options, name))
        push!(files, resp)
    end
    ftp_cleanup()
    return files
end
#################################################################################

#################################################################################
## Unzipping and loading files into memory                                      ##
#################################################################################
"""
    get_files(year, kind)

Unizips and loads the desires files from the zip file from the year and save it in
a Dataframe.

kind = :chemicals loads the file "chemical.txt", which contains a list with
all approved chemicals for each year and their chem_codes.
kind = :sites loads the file "site.txt", which contains a list with all sites
(commodities) that can be reported in CA for that year and their site_codes.
kind = :counties loads the the file "county.txt", which contains a list with
all counties in CA and their county_cd (codes).
kind = :products loads the files "product.txt", which contains a list with all approved
chemical products in CA for any year. The resulting dataframe contains
"""
function get_files(year::Int, kind::Symbol)
    for i in 1:size(files)[1]
        if files.year[i] == 2016
            reading = ZipFile.Reader(files.files[i].body)
            for i in 1:length(reading.files)
                if kind == :chemicals && reading.files[i].name == "pur2016/chemical.txt"
                    chemicals = CSV.File(IOBuffer(read(reading.files[i])))
                    return chemicals
                    break
                end
                if kind == :sites && reading.files[i].name == "pur2016/site.txt"
                    sites = CSV.File(IOBuffer(read(reading.files[i])))
                    return sites
                    break
                end
                if kind == :counties && reading.files[i].name == "pur2016/county.txt"
                    counties = CSV.File(IOBuffer(read(reading.files[i])))
                    return counties
                    break
                end
                if kind == :products && reading.files[i].name == "pur2016/product.txt"
                    products = CSV.File(IOBuffer(read(reading.files[i])))
                    return products
                    break
                end
            end
        end

        if files.year[i] == year
            reading = ZipFile.Reader(files.files[i].body)
            for i in 1:length(reading.files)
                if kind == :chemicals && reading.files[i].name == "chemical.txt"
                    chemicals = CSV.File(IOBuffer(read(reading.files[i])))
                    return chemicals
                    break
                end
                if kind == :sites && reading.files[i].name == "site.txt"
                    sites = CSV.File(IOBuffer(read(reading.files[i])))
                    return sites
                    break
                end
                if kind == :counties && reading.files[i].name == "county.txt"
                    counties = CSV.File(IOBuffer(read(reading.files[i])))
                    return counties
                    break
                end
                if kind == :products && reading.files[i].name == "product.txt"
                    products = CSV.File(IOBuffer(read(reading.files[i])))
                    return products
                    break
                end
            end
        end
    end
end

# Function to extract the differences between the approved chemicals between years
function compare_chem_years(year1::Int, year2::Int)
    join(get_files(maximum([year1, year2]), :chemicals),
         get_files(minimum([year1, year2]), :chemicals),
         on =:chem_code, kind = :anti)
end

# Function to extract the differences between the approved sites between years.
function compare_site_years(year1::Int, year2::Int)
    join(get_files(maximum([year1, year2]), :sites),
         get_files(minimum([year1, year2]), :sites),
         on =:site_code, kind = :anti)
end

function compare_product_years(year1::Int, year2::Int)
    join(get_files(maximum([year1, year2]), :products),
         get_files(minimum([year1, year2]), :products),
         on =:site_code, kind = :anti)
end

# find sites using any word (site) as a string.
function find_product_code(product::AbstractString, year::Int)
    products = get_files(year, :products)
    filtered = DataFrame(prod=[], site_name=[])
    for i in 1:size(sites,1)
        matching = match(Regex(uppercase(site)), sites[i,2])
        if matching != nothing
            push!(filtered, (site_code = sites[i,1], site_name=sites[i,2]))
        end
    end
    return filtered
end

# find sites using any word (site) as a string.
function find_site_code(site::AbstractString, year::Int)
    sites = get_files(year, :sites)
    filtered = DataFrame(site_code=[], site_name=[])
    for i in 1:size(sites,1)
        matching = match(Regex(uppercase(site)), sites[i,2])
        if matching != nothing
            push!(filtered, (site_code = sites[i,1], site_name=sites[i,2]))
        end
    end
    return filtered
end

# Extract the county code
function find_county_code(county_name::AbstractString, year::Int)
    counties = get_files(year, :counties)
    county_code = []
    for i in 1:length(counties[:couty_name])
        if counties[i,2] == uppercase(county_name)
            county_code = counties[i,1]
            break
        end
    end
    return county_code
end

################################################################################
## Dealing with the PUR.txt file                                              ##
################################################################################
# Function to load pur file from disk, using the county name as a string and the year
function load_pur(county_name::AbstractString, year::Int, dropmissing::Bool = true)
    county_code = find_county_code(county_name, year)

    string_year = string(year)

    pur = ()
    for i in 1:length(files.files)
        if files.year[i] == 2016
            reading = ZipFile.Reader(files.files[i].body)
            for i in 1:length(reading.files)
                if reading.files[i].name == "pur2016/udc" * string_year[3:4] * "_"* string(county_code) * ".txt"
                    pur = CSV.File(IOBuffer(read(reading.files[i])), escapechar='#',
                    types=Dict(18 => Union{Int64, Char, Missing},20 => Union{Int64, Char, Missing}))
                end
            end
        end
        if files.year[i] == year
            reading = ZipFile.Reader(files.files[i].body)
            for i in 1:length(reading.files)
                if reading.files[i].name == "udc" * string_year[3:4] * "_"* string(county_code) * ".txt"
                    pur = CSV.File(IOBuffer(read(reading.files[i])), escapechar='#',
                    types=Dict(18 => Union{Int64, Char, Missing},20 => Union{Int64, Char, Missing}))
                end
            end
        end
    end

    pur_df = DataFrame(pur)

    pur_df = @select(pur_df, :use_no, :prodno, :chem_code, :prodchem_pct, :lbs_chm_used,
    :lbs_prd_used, :amt_prd_used, :unit_of_meas, :acre_planted, :unit_planted,
    :acre_treated, :unit_treated, :applic_cnt, :applic_dt, :grower_id, :site_code)

    # add column for year
    pur_df[:year] = repeat([year], outer = size(pur_df, 1))

    # add chem name
    pur_df[:chem_name] = repeat(["0"],outer = size(pur_df, 1))

    if dropmissing == true
        pur_df = dropmissing!(pur_df)
    end

    chem_codes = get_files(year, :chemicals)
    chemcodes = Dict()
    for i in 1:size(chem_codes,1)
        push!(chemcodes, (chem_codes[i,1] => chem_codes[i,3]))
    end

    for i in 1:size(pur_df,1)
        chemical = pur_df[i,:chem_code]
        if typeof(chemical) != Missing
            pur_df[i,:chem_name] = chemcodes[chemical]
        end
    end
    return pur_df
end

# Select a site code using the find_site_code function
function select_site_code(df::DataFrame, site_code::Int)
    @where(df, :site_code .== site_code)
end

function select_insecticide(df::DataFrame, insecticide_name::AbstractString)
    insecticide_name = string(uppercase(insecticide_name))
    @where(df, :chem_name .== insecticide_name)
end

# Function to append the data from different years
function append_years(county_name::AbstractString, first_year::Int, last_year::Union{Nothing,Int},
                      dropmissing::Bool = true)
    if last_year==nothing
        total = load_pur(county_name, first_year, false)
        return total
    end

    if dropmissing == false
        total = load_pur(county_name, first_year, false)
        for i in first_year+1:last_year
            pur = load_pur(county_name, i, false)
            total= append!(total, pur)
        end
        return total
    end

    total = load_pur(county_name, first_year)
    for i in first_year+1:last_year
        pur = load_pur(county_name, i)
        total= append!(total, pur)
    end
    return total
end

# Function to obtain the information in insecticide use from a given commodity and county
function insecticide_use(county_name::AbstractString,
                         insecticide_name::AbstractString,
                         site_code::Int,
                         first_year::Int, last_year::Union{Nothing,Int},
                         dropmissing::Bool=true)
    total = append_years(county_name, first_year, last_year, dropmissing)
    total = select_site_code(total, site_code)
    total = select_insecticide(total, insecticide_name)
    total = by(total, [:year]) do df
    DataFrame(lbs_chm_used = sum(df[:lbs_chm_used]),
              N = size(df,1))
    end
    return total
end

function plot_insecticide_use(ounty_name::AbstractString,
                         insecticide_name::AbstractString,
                         site_code::Int,
                         first_year::Int, last_year::Union{Nothing,Int},
                         dropmissing::Bool=true)
    result = insecticide_use(county_name, insecticide_name, site_code, first_year, last_year,
                             dropmissing)
    plot(result, x = :year, y = :lbs_chm_used, Geom.line)
end
