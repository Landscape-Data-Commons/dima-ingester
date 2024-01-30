#!/bin/bash

tablearray=('tblPlots' 
      'tblLines' 
      'tblLPIDetail' 
      'tblLPIHeader' 
      'tblGapDetail' 
      'tblGapHeader' 
      'tblQualHeader' 
      'tblQualDetail' 
      'tblSoilStabHeader' 
      'tblSoilStabDetail' 
      'tblSoilPitHorizons' 
      'tblSoilPits' 
      'tblSpecRichHeader' 
      'tblSpecRichDetail' 
      'tblPlantProdHeader' 
      'tblPlantProdDetail' 
      'tblPlotNotes' 
      'tblPlantDenHeader' 
      'tblPlantDenDetail' 
      'tblSpecies' 
      'tblSpeciesGeneric' 
      'tblSites' 
      'tblBSNE_Box' 
      'tblBSNE_BoxCollection' 
      'tblBSNE_Stack' 
      'tblBSNE_TrapCollection' 
      'tblCompactDetail' 
      'tblCompactHeader' 
      'tblDKDetail' 
      'tblDKHeader' 
      'tblDryWtCompYield' 
      'tblDryWtDetail' 
      'tblDryWtHeader' 
      'tblESDDominantPerennialHeights' 
      'tblESDRockFragments' 
      'tblESDWaypoints' 
      'tblInfiltrationDetail' 
      'tblInfiltrationHeader' 
      'tblLICDetail' 
      'tblLICHeader' 
      'tblLICSpecies' 
      'tblNestedFreqDetail' 
      'tblNestedFreqHeader' 
      'tblNestedFreqSpeciesDetail' 
      'tblNestedFreqSpeciesSummary' 
      'tblOcularCovDetail' 
      'tblOcularCovHeader' 
      'tblPlantDenQuads' 
      'tblPlantDenSpecies' 
      'tblPlantLenDetail' 
      'tblPlantLenHeader' 
      'tblPlotHistory' 
      'tblPTFrameDetail' 
      'tblPTFrameHeader' 
      'tblQualDetail' 
      'tblQualHeader' 
      'tblSpeciesGrowthHabits' 
      'tblSpeciesRichAbundance' 
      'tblTreeDenDetail' 
      'tblTreeDenHeader'
)

contains_true (){
     local array=$1[@]
     local seeking=$2
     local in=1
     for element in "${!array}"; 
     do
          if [[ $element == "$seeking" ]]; 
          then 
               in=0
               break
          fi
     done 
     return $in
}

# for each file in x directory:
## for each table inside each file:
### mdb-csv to xdirectory/file-table 

# input files in mdbs directory
for mdbfile in /usr/src/dimas/*;
  do
    if [[ $mdbfile =~ \.mdb$ ]]; then
    # mdb_tables is an array 
    echo "found mdb file" 

    declare "mdb_tables=$( mdb-tables "$mdbfile" )" 

    # trimming file extension from filename
    declare "no_extension=${mdbfile%.*}"

    # trimming whitespace from filename
    declare "no_whitespace=$(echo "$no_extension" | tr -d ' ')"

    # trimming path from filename
    declare "no_path=${no_whitespace##*/}"


    echo "Currently working on mdb: ${no_path}"
  # if dir does not exist, create it
    [ -d "/usr/src/dimas/extracted_${no_path}" ] || mkdir "/usr/src/dimas/extracted_${no_path}"
    echo "File is: ${mdbfile}"
    for table in $mdb_tables
      do
        echo "Working on table: $table from mdb: $mdbfile"
        # for each mdb, if the table exists on the main array = extract to csv
        contains_true tablearray "$table" && mdb-export -D '%Y-%m-%d %H:%M:%S' "$mdbfile" "$table" > \
        "/usr/src/dimas/extracted_${no_path}/${no_path}-${table}.csv" || echo "$table NOT EXPORTED"
      done
    fi
  done
