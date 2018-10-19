################################################################################
## Loading packages that are used.                                            ##
################################################################################
using CSV, DataFrames, DataFramesMeta
using Gadfly
using ZipFile
#################################################################################

#################################################################################
## Loading data from CDPR FTP server.                                          ##
#################################################################################
"""
    choose_years(year1[, year2])

Use this function to start analysis, by choosing the years you want to use for your study.
This will create the proper filepaths.
"""
function choose_years(year1::Int, year2=Union{Nothing,Int})
    if year2 != nothing
        first_year = minimum([year1, year2])
        second_year = maximum([year1, year2])
        years = collect(UnitRange(first_year, second_year))
    else
        years = [year1]
    end
end
#################################################################################

#################################################################################
## Unzipping and loading files into memory                                      ##
#################################################################################
function uncompress_file(year::Int)
    path = "Pesticide_Use/pur" * string(year) *".zip"
    io = open(path, "r")
    io = IOBuffer(read(io))
    fl = ZipFile.Reader(io)
    return fl
end
"""
    get_files(:kind, year)

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
function get_files(kind::Symbol, year::Int)
    reading = uncompress_file(year)
    # the zile file for 2016 has and extra "pur2016/" on the file name that has to be removed
    if year == 2016
        for i in 1:length(reading.files)
            (Y, N) = split(reading.files[i].name, "/")
            reading.files[i].name = N
        end
    end

    for i in 1:length(reading.files)
        if kind == :chemicals && reading.files[i].name == "chemical.txt"
            fl = CSV.read(IOBuffer(read(reading.files[i])))
            return fl
        elseif kind == :sites && reading.files[i].name == "site.txt"
            fl = CSV.read(IOBuffer(read(reading.files[i])))
            return name
        elseif kind == :counties && reading.files[i].name == "county.txt"
            fl = CSV.read(IOBuffer(read(reading.files[i])))
            return fl
        elseif kind == :products && reading.files[i].name == "product.txt"
            fl = CSV.read(IOBuffer(read(reading.files[i])))
            return fl
        end
    end
    throw("Wrong symbol for kind")
end

# experimental function. need to time with BenchmarkTools
function get_files1(kind::Symbol, year::Int)
    reading = uncompress_file(year)
    # the zile file for 2016 has and extra "pur2016/" on the file name that has to be removed
    if year == 2016
        for i in 1:length(reading.files)
            (Y, N) = split(reading.files[i].name, "/")
            reading.files[i].name = N
        end
    end

    if kind == :chemicals
        for i in 1:length(reading.files)
            if reading.files[i].name == "chemical.txt"
                name = CSV.read(IOBuffer(read(reading.files[i])))
                return name
            end
        end
    elseif kind == :sites
        for i in 1:length(reading.files)
            if reading.files[i].name == "site.txt"
                name = CSV.read(IOBuffer(read(reading.files[i])))
                return name
            end
        end
    elseif kind == :counties
        for i in 1:length(reading.files)
            if reading.files[i].name == "county.txt"
                name = CSV.read(IOBuffer(read(reading.files[i])))
                return name
            end
        end
    elseif kind == :products
        for i in 1:length(reading.files)
            if reading.files[i].name == "product.txt"
                name = CSV.read(IOBuffer(read(reading.files[i])))
                return name
            end
        end
    end
    throw("Wrong symbol for kind")
end

# Function to extract the differences between the approved chemicals between years
function compare_info_years(kind::Symbol, year1::Int, year2::Int)
    if kind == :chemicals
        join(get_files(maximum([year1, year2]), :chemicals),
            get_files(minimum([year1, year2]), :chemicals),
            on =:chem_code, kind = :anti)
    elseif kind == :sites
        join(get_files(maximum([year1, year2]), :sites),
             get_files(minimum([year1, year2]), :sites),
             on =:site_code, kind = :anti)
    elseif kind == :products
        join(get_files(maximum([year1, year2]), :products),
             get_files(minimum([year1, year2]), :products),
             on =:prodno, kind = :anti)
    else throw("Wrong symbol for kind")
    end
end

# find sites using any word (site) as a string.
function find_product_code(product::AbstractString, year::Int)
    products = get_files(:products, year)
    filtered = DataFrame(prodno=[], product_name=[])
    for i in 1:size(products,1)
        matching = match(Regex(uppercase(product)), products[i,8])
        if matching != nothing
            push!(filtered, (prodno = products[i,1], product_name=products[i,8]))
        end
    end
    if nrow(filtered) != 0
        return filtered
    else throw("Non-existent product")
    end
end

# find sites using any word (site) as a string.
function find_site_code(site::AbstractString, year::Int)
    sites = get_files(:sites, year)
    filtered = DataFrame(site_code=[], site_name=[])
    for i in 1:size(sites,1)
        matching = match(Regex(uppercase(site)), sites[i,2])
        if matching != nothing
            push!(filtered, (site_code = sites[i,1], site_name=sites[i,2]))
        end
    end
    if nrow(filtered) != 0
        return filtered
    else throw("Non-existent commodity")
    end
end

function find_chem_code(chemical::AbstractString, year::Int)
    chemicals = get_files(:chemicals, year)
    filtered = DataFrame(chem_code=[], chem_name=[])
    for i in 1:size(chemicals,1)
        matching = match(Regex(uppercase(chemical)), chemicals[i,3])
        if matching != nothing
            push!(filtered, (chem_code = chemicals[i,1], chem_name=chemicals[i,3]))
        end
    end
    if nrow(filtered) != 0
        return filtered
    else throw("Non-existent chemical")
    end
end

# Extract the county code
function find_county_code(county_name::AbstractString, year::Int)
    counties = get_files(year, :counties)
    for i in 1:length(counties[:couty_name])
        if counties[i,2] == uppercase(county_name)
            county_code = counties[i,1]
            return county_code
        end
    end
    throw("Wrong county name")
end

################################################################################
## Dealing with the PUR.txt file                                              ##
################################################################################
# Function to load pur file from disk, using the county name as a string and the year
function load_pur(county_name::AbstractString, year::Int, dropmissing::Bool = true)
    county_code = find_county_code(county_name, year)
    string_year = string(year)

    reading = uncompress_file(year)
    # the zip file for 2016 has and extra "pur2016/" on the file name that has to be removed
    if year == 2016
        for i in 1:length(reading.files)
            (Y, N) = split(reading.files[i].name, "/")
            reading.files[i].name = N
        end
    end

    for i in 1:length(reading.files)
        if reading.files[i].name == "udc" * string_year[3:4] * "_"* string(county_code) * ".txt"
            pur = CSV.File(IOBuffer(read(reading.files[i])), escapechar='#',
            types=Dict(18 => Union{Int64, Char, Missing},20 => Union{Int64, Char, Missing}))
            pur_df = DataFrame(pur)
        end
    end

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

function select_insecticide(df::DataFrame, insecticide_name::Union{AbstractString, Int})
    if typeof(insecticide_name)== AbstractString
        insecticide_name = string(uppercase(insecticide_name))
        @where(df, :chem_name .== insecticide_name)
    elseif typeof(insecticide_name)==Int
        df = dropmissing!(df)
        @where(df, :chem_code .== insecticide_name)
    else throw("Wrong insecticide name or code")
    end
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
                         insecticide_name::Union{AbstractString, Int},
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

function plot_insecticide_use(county_name::AbstractString,
                         insecticide_name::AbstractString,
                         site_code::Int,
                         first_year::Int, last_year::Union{Nothing,Int},
                         dropmissing::Bool=true)
    result = insecticide_use(county_name, insecticide_name, site_code, first_year, last_year,
                             dropmissing)
    plot(result, x = :year, y = :lbs_chm_used, Geom.line)
end

plot(abamectin_monterey_154, x = :year, y = :lbs_chm_used, Geom.line, Theme(background_color="white"))
