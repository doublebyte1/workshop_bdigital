/***********************************************************************************************************
Improvements:
- uncomment the remove shell commands, and add a test to check if files/dirs exist (it fails, otherwise) ///
- use the path of th csv as an argument
*/

-- remove directories and files, if they exist

--fs -rm  /home/hduser/data/data_na/*;
--fs -rm  /home/hduser/data/data_pol/*;
--fs -rm  /home/hduser/data/data_utm/*;
--fs -rm  /home/hduser/data/data_wgs84/*;

--fs -rmdir  /home/hduser/data/data_na/;
--fs -rmdir  /home/hduser/data/data_pol/;
--fs -rmdir  /home/hduser/data/data_utm/;
--fs -rmdir  /home/hduser/data/data_wgs84/;

-- load data from /home/hduser/data/accidents_id.tsv
data = LOAD 'hdfs://localhost:54310/home/hduser/data/accident_data.tsv'
            USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')      
            AS (id :chararray, Any :long, mes :long, dia :long, C_HORA_TRUCADA, D_COORD_GEO_IMPACTE :chararray);

/* The original dataset is split into 4 subsets (na,utm,wgs84 and police). This is done with incremental filters, so that we ensure there are no values left behind due to overlapping filters. Each coordinate set, is created with an id, so that we can merge it back to the original matrix*/

--filter original dataset for na values (pattern: alphabet letters, slashes, 77)
na= filter data by REGEX_EXTRACT(D_COORD_GEO_IMPACTE,'[A-z]',0) is not null or D_COORD_GEO_IMPACTE=='77' or INDEXOF(D_COORD_GEO_IMPACTE,'/',0)>=0;
na_ = FOREACH na GENERATE id, null as lon:double, null as lat:double;
new_na = join na by id, na_ by id;

--remove na values from the subset with a negation of the previous filter
clean = filter data by REGEX_EXTRACT(D_COORD_GEO_IMPACTE,'[A-z]',0) is null and D_COORD_GEO_IMPACTE!='77' and INDEXOF(D_COORD_GEO_IMPACTE,'/',0)<0;

/*filter subset for wgs84 values (pattern: a '.' on the second character, that corresponds to the decimal place on latitude). The string is split into a pair of values, and the x and y are swapped; each trimmed value is converted into double;*/
wgs84= filter clean by INDEXOF(D_COORD_GEO_IMPACTE,'.',0)==2;
wgs84_ = FOREACH wgs84 GENERATE id, FLATTEN(STRSPLIT(D_COORD_GEO_IMPACTE,'[ ,]',2)) AS (lat:chararray,lon:chararray);
coords = FOREACH wgs84_ GENERATE id, (double)TRIM(lon) as lon:double, (double)TRIM(lat) as lat:double;
new_wgs84 = join wgs84 by id, coords by id;

--remove wgs84 values from the subset with a negation of the previous filter
grid = filter clean by INDEXOF(D_COORD_GEO_IMPACTE,'.',0)!=2;

/*filter subset for utm values (pattern: a comma to separate values and a decimal separator). The string is split into a pair of values, that are trimmed and converted to doubles */
utm= filter grid by  INDEXOF(D_COORD_GEO_IMPACTE,',',0) >= 0 or INDEXOF(D_COORD_GEO_IMPACTE,'.',0) >= 0;
utm_ = FOREACH utm GENERATE id, FLATTEN(STRSPLIT(D_COORD_GEO_IMPACTE,'[ ,]',2)) AS (lon:chararray,lat:chararray);
coords = FOREACH utm_ GENERATE id, (double)TRIM(lon) as lon:double, (double)TRIM(lat) as lat:double;
new_utm = join utm by id, coords by id;

--remove utm values from the subset with a negation of the previous filter; in theory, the rest of the values are the UTM encoded coordinates, registered by the police. Each long,lat pair is submitted to a transformation, in order to extract the UTM coordinates */
rest = filter grid by  INDEXOF(D_COORD_GEO_IMPACTE,',',0) <0 and INDEXOF(D_COORD_GEO_IMPACTE,'.',0) < 0;

pol = rest;--filter rest by  INDEXOF(D_COORD_GEO_IMPACTE,',',0) <0;
pol_ = FOREACH pol GENERATE id, FLATTEN(STRSPLIT(D_COORD_GEO_IMPACTE,'[ ,]',2)) AS (lon:chararray,lat:chararray);
coords = FOREACH pol_ GENERATE id, (double)TRIM(lon)/1000+400000 as lon:double, (double)TRIM(lat)/1000+4500000 as lat:double;
new_pol = join pol by id, coords by id;

--export everything (na is optional, really)  
store new_wgs84 into '/home/hduser/data/data_wgs84' using PigStorage('\t','-schema');
store new_utm into '/home/hduser/data/data_utm' using PigStorage('\t','-schema');
store new_pol into '/home/hduser/data/data_pol' using PigStorage('\t','-schema');

store new_na into '/home/hduser/data/data_na' using PigStorage('\t','-schema');
    
    
    