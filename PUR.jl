################################################################################
## Loading packages that are used.                                            ##
################################################################################
using CSV, DataFrames, DataFramesMeta
using Gadfly
using ZipFile

# use ; in the REPL to move to shell mode
# change working directory
# cd "/Users/alejandromerchan/Pesticide_Use"
using FTPClient
function download_info(year1::Int, year2=nothing)
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

################################################################################
## Dealing with chemicals                                                     ##
################################################################################
# Function that loads different files from the zip files for a certain year in
# a dataframe.
# kind = :chemicals loads the file "chemical.txt", which contains a list with
# all approved chemicals for each year and their chem_codes.
# kind = :sites loads the file "site.txt", which contains a list with all sites
# (commodities) that can be reported in CA for that year and their site_codes.
# kind = :counties loads the the file "county.txt", which contains a list with
# all counties in CA and their county_cd (codes).
function get_files(year::Int, kind::Symbol)
    for i in 1:length(files.files)
        if files.year[i] == year
            reading = ZipFile.Reader(files.files[i].body)
            for i in 1:length(reading.files)
                if kind == :chemicals && reading.files[i].name == "chemical.txt"
                    chemicals = CSV.read(IOBuffer(read(reading.files[i])))
                    return chemicals
                    break
                end
                if kind == :sites && reading.files[i].name == "site.txt"
                    sites = CSV.read(IOBuffer(read(reading.files[i])))
                    return sites
                    break
                end
                if kind == :counties && reading.files[i].name == "county.txt"
                    counties = CSV.read(IOBuffer(read(reading.files[i])))
                    return counties
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

# find sites using any word (site) as a string.
function find_site_code(year::Int, site::AbstractString)
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

################################################################################
## Dealing with the PUR.txt file                                              ##
################################################################################
# Function to load file from disk
# Takes the county code from the dict

function load_pur(county_name::AbstractString, year::Int)
    # Creates the dictionary with county codes
    counties = get_files(year, :counties)
    # Extract the county code
    county_code = []
    for i in 1:length(counties[:couty_name])
        if counties[i,2] == uppercase(county_name)
            county_code = counties[i,1]
            break
        end
    end
    # transform year into string
    string_year = string(year)
    pur = DataFrame()
    for i in 1:length(files.files)
        if files.year[i] == year
            reading = ZipFile.Reader(files.files[i].body)
            for i in 1:length(reading.files)
                if reading.files[i].name == "udc" * string_year[3:4] * "_"* string(county_code) * ".txt"
                    pur = CSV.read(IOBuffer(read(reading.files[i])), escapechar = "#")
                end
            end
        end
    end
    # add column for year
    pur[:year] = repeat([year], outer = size(pur, 1))

    # add chem name
    pur[:chem_name] = repeat(["0"],outer = size(pur, 1))

    pur = @select(pur, :use_no, :prodno, :chem_code, :prodchem_pct, :lbs_chm_used,
    :lbs_prd_used, :amt_prd_used, :unit_of_meas, :acre_planted, :unit_planted,
    :acre_treated, :unit_treated, :applic_cnt, :applic_dt, :grower_id, :site_code,
    :year, :chem_name)

    pur = dropmissing!(pur)

    chem_codes = get_files(year, :chemicals)
    chemcodes = Dict()

    for i in 1:size(chem_codes,1)
        push!(chemcodes, (chem_codes[i,1] => chem_codes[i,3]))
    end

    for i in 1:size(pur,1)
         chemical = pur[i,:chem_code]
         pur[i,:chem_name] = chemcodes[chemical]
    end
    return pur
end

# Select a site code using the find_site_code function
function select_site_code(df::DataFrame, site_code::Int)
    @where(df, :site_code .== site_code)
end

function select_insecticide(df::DataFrame, insecticide_name::AbstractString)
    insecticide_name = string(uppercase(insecticide_name))
    @where(df, :chem_name .== insecticide_name)
end

# First load the zip files for all different years using the download_info
# function
function insecticide_use(county_name::AbstractString,
                         insecticide_name::AbstractString,
                         site_code::Int,
                         first_year::Int, last_year::Int)
    total = load_pur(county_name, first_year)
    for i in first_year+1:last_year
        pur = load_pur(county_name, i)
        total= append!(total, pur)
    end
    total = select_site_code(total, site_code)
    total = select_insecticide(total, insecticide_name)
    total = by(total, [:year]) do df
    DataFrame(lbs_chm_used = sum(df[:lbs_chm_used]),
              N = size(df,1))
    end
    return total
end

function plot_insecticide_use(county_name::AbstractString,
                         insecticide_name::AbstractString,
                         first_year::Int, last_year::Int)
    result = insecticide_use(county_name, insecticide_name,
                              first_year, last_year)
    plot(result, x = :year, y = :lbs_chm_used, Geom.line)
end
