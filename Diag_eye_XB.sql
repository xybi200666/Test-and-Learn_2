------------------------------------------------------------------------------------------------------------------------------------
CREATE COLUMN FOR DIAG EYE - 
1 = right
2 = left
3 = bilateral

cdr.patientproblem:        practicedescription (2), description (1), problemcomment (5)
cdr.patientproblemhistory: targetsitetext (3), targetsitecode, targetsiteuid, mastertargetsiteuid (4 for the three columns)


rules: (1) ICD10
       (2) if no ICD10, i.e., ICD9, then relying on key words searching
       

Notes: practicedescription for e1|e2 only returns 320 records for all problem and history tables
-------------------------------------------------------------------------------------------------------------------------------------





/*-----------------------------------------------------------------------------------------------------------------------------
            pull out ICD9 and ICD10 by merging cdr_parquet.patientproblem with cdr_parquet.patientproblemhistory
------------------------------------------------------------------------------------------------------------------------------*/ 

select distinct pp.patientUID, pp.PracticeCode, 	
			   pph.problemonsetdate         as DiagDate         ,               
			   pph.documentationdate as DocumentationDate 
	  
from (select * from cdr_parquet.patientproblem where patientUID in (select patientUID from cdr_nvs001.step1_laterality_final)  )  pp
inner join cdr_parquet.patientproblemhistory pph
on pp.patientproblemuid = pph.patientproblemuid
WHERE 
	  /* nAMD */      
	  regexp_like(trim(upper(pp.PracticeCode)), '^362.52|H35.32|H35.3210|H35.3211|H35.3212|H35.3213|H35.3220|H35.3221|H35.3222|H35.3223|H35.3230|H35.3231|H35.3232|H35.3233|H35.3290|H35.3291|H35.3292|H35.3293') or

	/* macular edema following retinal vein occlusion */ 
		regexp_like(trim(upper(pp.PracticeCode)), '^362.35|362.36|H34.81|H34.83')  or 

		/* diabetic macular edema */
		regexp_like(trim(upper(pp.PracticeCode)), '^362.07|E08.321|E08.331|E08.341|E08.351|E09.321|E09.331|E09.341|E09.351|E10.321|E10.331|E10.341|E10.351|E11.321|E11.331|E11.341|E11.351|E13.321|E13.331|E13.341|E13.351') or 

		/* diabetic retinopathy */
		regexp_like(trim(upper(pp.PracticeCode)), '^250.5|362.01|362.02|362.03|362.04|362.05|362.06|250.50|250.51|250.52|250.53|E08.32|E09.32|E11.32|E13.32|E08.33|E09.33|E11.33|E13.33|E08.34|E09.34|E11.34|E13.34|E08.35|E09.35|E10.35|E11.35|E13.35|E08.31|E09.31|E10.31|E11.31|E13.31')  or

		/*myopic choroidal neovascularization  */
		regexp_like(trim(upper(pp.PracticeCode)), '^H35.05|H44.2A|362.16')
				
				
				
CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_five_conditions(
    patientUID STRING,  practicecode STRING, DiagDate STRING, DocumentationDate STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_five_conditions/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')


/*-----------------------------------------------------------------------------------------------------------------------------
          create multiple columns based on ICDs
------------------------------------------------------------------------------------------------------------------------------*/ 

		
select distinct pp.patientUID, 
		case when regexp_like(trim(upper(pp.PracticeCode)), '^362.52|362.35|362.36|362.07|250.5|362.01|362.04|362.05|362.06|250.50|250.51|250.52|250.53|362.16|362.02|362.03') then 'ICD9'
			 when regexp_like(trim(upper(pp.PracticeCode)), '^H35.32|H35.3210|H35.3211|H35.3212|H35.3213|H35.3220|H35.3221|H35.3222|H35.3223|H35.3230|H35.3231|H35.3232|
			 |H35.3233|H35.3290|H35.3291|H35.3292|H35.3293|H34.81|H34.83|E08.321|E08.331|E08.341|E08.351|E09.321|E09.331|E09.341|E09.351|E10.321|E10.331|E10.341|E10.351|
			 |E11.321|E11.331|E11.341|E11.351|E13.321|E13.331|E13.341|E13.351|E08.32|E09.32|E11.32|E13.32|E08.33|E09.33|E11.33|E13.33|E08.34|E09.34|E11.34|E13.34|
			 |E08.35|E09.35|E10.35|E11.35|E13.35|E08.31|E09.31|E10.31|E11.31|E13.31') then 'ICD10'
			end as ICD_version,

			  /* create an indicator for each condition */
			  case when regexp_like(trim(upper(pp.PracticeCode)), '^362.52|H35.32|H35.3210|H35.3211|H35.3212|H35.3213|
			  |H35.3220|H35.3221|H35.3222|H35.3223|H35.3230|H35.3231|H35.3232|H35.3233|H35.3290|H35.3291|H35.3292|H35.3293')  then '1' else '0' end as nAMD,
													  
			  case when regexp_like(trim(upper(pp.PracticeCode)), '^362.35|362.36|H34.81|H34.83')  then '1' else '0' end as macular_edema,
	  
			  case when regexp_like(trim(upper(pp.PracticeCode)), '^362.07|E08.321|E08.331|E08.341|E08.351|E09.321|E09.331|E09.341|E09.351|E10.321|
			  |E10.331|E10.341|E10.351|E11.321|E11.331|E11.341|E11.351|E13.321|E13.331|E13.341|E13.351')  then '1' else '0' end as DME,
										  
			  case when regexp_like(trim(upper(pp.PracticeCode)), '^250.5|362.01|362.04|362.05|362.06|250.50|250.51|250.52|250.53|
			  |E08.32|E09.32|E11.32|E13.32|E08.33|E09.33|E11.33|E13.33|E08.34|E09.34|E11.34|E13.34|362.02|
			  |E08.35|E09.35|E10.35|E11.35|E13.35|362.03|E08.31|E09.31|E10.31|E11.31|E13.31')  then '1' else '0' end as diabetic_ret,
										  
			  case when regexp_like(trim(upper(pp.PracticeCode)), '^H35.05|H44.2A|362.16')  then '1' else '0' end as MCN,
	  
	  
			  /* create diag_eye field */
			  /*-- 1st recode eyes based on ICD-10CM-- */ 
			CASE WHEN regexp_like(upper(pp.PracticeCode), '^H35.3210|H35.3211|H35.3212|H35.3213|
			|H34.811|H34.831|H44.2A1|H35.051|H35.321|E08.3211|E08.3311|E08.3411|E08.3511|E09.3211|E09.3311|E09.3411|E09.3511|
														   |E10.3211|E10.3311|E10.3411|E10.3511|E11.3211|E11.3311|E11.3411|E11.3511|E13.3211|E13.3311|E13.3411|E13.3511|E08.3291|
														   |E08.3391|E08.3491|E08.3591|E09.3291|E09.3391|E09.3491|E09.3591|E10.3291|E10.3391|E10.3491|E10.3591|E11.3291|
														   |E11.3391|E11.3491|E11.3591|E13.3291|E13.3391|E13.3491|E13.3591|H34.8310|H34.8110') THEN '1'
				 WHEN regexp_like(upper(pp.PracticeCode), '^H35.3220|H35.3221|H35.3222|H35.3223|H34.812|H34.832|H44.2A2|H35.052|H35.322|E08.3212|E08.3312|E08.3412|E08.3512|E09.3212|E09.3312|E09.3412|E09.3512|E10.3212|E10.3312|E10.3412|E10.3512|E11.3212|E11.3312|E11.3412|E11.3512|E13.3212|E13.3312|E13.3412|E13.3512|E08.3292|
														   |E08.3392|E08.3492|E08.3592|E09.3292|E09.3392|E09.3492|E09.3592|E10.3292|E10.3392|E10.3492|E10.3592|E11.3292|E11.3392|
														   |E11.3492|E11.3592|E13.3292|E13.3392|E13.3492|E13.3592|H34.8320|H34.8120') THEN '2'
				 WHEN regexp_like(upper(pp.PracticeCode), '^H35.3230|H35.3231|H35.3232|H35.3233|
														  |H34.813|H34.833|H44.2A3|H35.053|H35.323|E08.3213|E08.3313|E08.3413|E08.3513|E09.3213|E09.3313|E09.3413|E09.3513|E10.3213|
														  |E10.3313|E10.3413|E10.3513|E11.3213|E11.3313|E11.3413|E11.3513|E13.3213|E13.3313|E13.3413|E13.3513|E08.3293|E08.3393|
														  |E08.3493|E08.3593|E09.3293|E09.3393|E09.3493|E09.3593|E10.3293|E10.3393|E10.3493|E10.3593|E11.3293|E11.3393|E11.3493|E11.3593|
														  |E13.3293|E13.3393|E13.3493|E13.3593|H34.8330|H34.8130') THEN '3'
			end as DiagEYE, 
	
			   pp.DiagDate         ,               
			   pp.DocumentationDate 
	  
		from cdr_nvs001.step5_five_conditions  pp
		
		
				
CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_five_conditions_2(
    patientUID STRING, ICD_version STRING, 
    nAMD STRING, macular_edema STRING, DME STRING, diabetic_ret STRING, 
    MCN STRING, DiagEYE STRING, DiagDate STRING, DocumentationDate STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_five_conditions_2/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')
		
		


/*-----------------------------------------------------------------------------------------------------------------------------
            join with cdr_nvs001.step1_laterality_final
------------------------------------------------------------------------------------------------------------------------------*/ 

       
select distinct b.patientUID, /*b.Inject_Eye, b.cpt_eye, b.HCPCS_EYE, b.inject_date, b.practicecode, b.CPT_code, */
        a.ICD_version, a.nAMD, a.macular_edema, a.DME, a.diabetic_ret, a.MCN, a.DiagEYE, a.DiagDate, a.DocumentationDate
from cdr_nvs001.step5_five_conditions_2       a
right join cdr_nvs001.step1_laterality_final  b
on a.patientUID=b.patientUID		



		
CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_diag_eye(
    patientUID STRING, 
    /* Inject_Eye STRING, cpt_eye STRING, HCPCS_EYE STRING, 
    inject_date STRING, practicecode STRING, CPT_code STRING, */
    ICD_version STRING, nAMD STRING, macular_edema STRING, DME STRING, diabetic_ret STRING, 
    MCN STRING, DiagEYE STRING, DiagDate STRING, DocumentationDate STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_diag_eye/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')
		
		
		
/*-----------------------------------------------------------------------------------------------------------------------------
            diag_eye based on key words searching
------------------------------------------------------------------------------------------------------------------------------*/	

/*------------------------------------------------
 key words searching 1: pp.practicedescription
---------------------------------------------------*/

SELECT DISTINCT pp.patientUID,  
	   pph.problemonsetdate         as DiagDate         ,               
	   pph.documentationdate as DocumentationDate,	   
	   pp.practicedescription

from (select a.patientUID, a.patientproblemuid, a.practicedescription, a.problemcomment, a.description
      from cdr_parquet.patientproblem          a
	  inner join cdr_nvs001.step5_diag_eye     b   /* only include those records withtout eye assigned based on ICD10s */
	  on a.patientUID=b.patientUID
	  where b.DiagEYE not in ('1','2','3')  ) pp
inner join cdr_parquet.patientproblemhistory pph
on pp.patientproblemuid = pph.patientproblemuid
where      regexp_like(lower(pp.practicedescription),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000|os|lt|e1|e2|left|le|362503005|ou|50|bilat|both|362508001|244486005)(\b+)')




CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_practicedescription(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    practicedescription STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_practicedescription/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')




select DISTINCT patientUID, DiagDate, DocumentationDate,
		CASE WHEN     regexp_like(lower(practicedescription),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000)(\b+)') then 1 else 0 end as practicedescription_1,
		CASE WHEN     regexp_like(lower(practicedescription),  '(\b+)(no ){0}(os|lt|e1|e2|left|le|362503005)(\b+)') then 1 else 0 end as practicedescription_2,
		CASE WHEN     regexp_like(lower(practicedescription),  '(\b+)(no ){0}(ou|50|bilat|both|362508001|244486005)(\b+)') then 1 else 0 end as practicedescription_3
from 
(
SELECT *,
      case when regexp_like(lower(practicedescription),  '(\b+)(no ){0}(right|left|both)(\b+)') and 
                not regexp_like(lower(practicedescription),  '(\b+)(eye|eyes)(\b+)') and 
                not regexp_like(lower(practicedescription),  '(\b+)(od|rt|e3|e4|re|362502000|os|lt|e1|e2|le|362503005|ou|50|bilat|362508001|244486005)(\b+)') and 
                regexp_like(lower(practicedescription),  '(\b+)(ear|leg|knee|arm|shoulder|face|head|nose|foot|hand|finger|mouth|neck|wrist|joint|toe)(s){0,1}(\b+)')
                then 1
                else 0 
                end as drop_record
from cdr_nvs001.step5_practicedescription
)
where drop_record=0




CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_practicedes_2(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    practicedescription_1 STRING, practicedescription_2 STRING, practicedescription_3 STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_practicedes_2/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')
 
 

/*------------------------------------------------
 key words searching 2: pp.problemcomment
---------------------------------------------------*/

SELECT DISTINCT pp.patientUID,  
	   pph.problemonsetdate         as DiagDate         ,               
	   pph.documentationdate as DocumentationDate,	   
	    pp.problemcomment

from (select a.patientUID, a.patientproblemuid, a.problemcomment
      from cdr_parquet.patientproblem          a
	  inner join cdr_nvs001.step5_diag_eye     b   /* only include those records withtout eye assigned based on ICD10s */
	  on a.patientUID=b.patientUID
	  where b.DiagEYE not in ('1','2','3')  ) pp
inner join cdr_parquet.patientproblemhistory pph
on pp.patientproblemuid = pph.patientproblemuid
where  regexp_like(lower(pp.problemcomment),       '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000|os|lt|e1|e2|left|le|362503005|ou|50|bilat|both|362508001|244486005)(\b+)')
		

CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_problemcomment(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    problemcomment STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_problemcomment/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')


select DISTINCT patientUID, DiagDate, DocumentationDate,
		CASE WHEN     regexp_like(lower(problemcomment),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000)(\b+)') then 1 else 0 end as problemcomment_1,
		CASE WHEN     regexp_like(lower(problemcomment),  '(\b+)(no ){0}(os|lt|e1|e2|left|le|362503005)(\b+)') then 1 else 0 end as problemcomment_2,
		CASE WHEN     regexp_like(lower(problemcomment),  '(\b+)(no ){0}(ou|50|bilat|both|362508001|244486005)(\b+)') then 1 else 0 end as problemcomment_3
from 
(
SELECT *,
      case when regexp_like(lower(problemcomment),  '(\b+)(no ){0}(right|left|both)(\b+)') and 
                not regexp_like(lower(problemcomment),  '(\b+)(eye|eyes)(\b+)') and 
                not regexp_like(lower(problemcomment),  '(\b+)(od|rt|e3|e4|re|362502000|os|lt|e1|e2|le|362503005|ou|50|bilat|362508001|244486005)(\b+)') and 
                regexp_like(lower(problemcomment),  '(\b+)(ear|leg|knee|arm|shoulder|face|head|nose|foot|hand|finger|mouth|neck|wrist|joint|toe)(s){0,1}(\b+)')
                then 1
                else 0 
                end as drop_record
from cdr_nvs001.step5_problemcomment
)
where drop_record=0


CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_problemcomment_2(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    problemcomment_1 STRING, problemcomment_2 STRING, problemcomment_3 STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_problemcomment_2/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')




/*------------------------------------------------
 key words searching 3: pp.description
---------------------------------------------------*/

SELECT DISTINCT pp.patientUID,  
	   pph.problemonsetdate         as DiagDate         ,               
	   pph.documentationdate as DocumentationDate,	   
	    pp.description

from (select a.patientUID, a.patientproblemuid, a.description
      from cdr_parquet.patientproblem          a
	  inner join cdr_nvs001.step5_diag_eye     b   /* only include those records withtout eye assigned based on ICD10s */
	  on a.patientUID=b.patientUID
	  where b.DiagEYE not in ('1','2','3')  ) pp
inner join cdr_parquet.patientproblemhistory pph
on pp.patientproblemuid = pph.patientproblemuid
where  regexp_like(lower(pp.description),       '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000|os|lt|e1|e2|left|le|362503005|ou|50|bilat|both|362508001|244486005)(\b+)')
		

CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_description(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    description STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_description/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')


select DISTINCT patientUID, DiagDate, DocumentationDate,
		CASE WHEN     regexp_like(lower(description),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000)(\b+)') then 1 else 0 end as description_1,
		CASE WHEN     regexp_like(lower(description),  '(\b+)(no ){0}(os|lt|e1|e2|left|le|362503005)(\b+)') then 1 else 0 end as description_2,
		CASE WHEN     regexp_like(lower(description),  '(\b+)(no ){0}(ou|50|bilat|both|362508001|244486005)(\b+)') then 1 else 0 end as description_3
from 
(
SELECT *,
      case when regexp_like(lower(description),  '(\b+)(no ){0}(right|left|both)(\b+)') and 
                not regexp_like(lower(description),  '(\b+)(eye|eyes)(\b+)') and 
                not regexp_like(lower(description),  '(\b+)(od|rt|e3|e4|re|362502000|os|lt|e1|e2|le|362503005|ou|50|bilat|362508001|244486005)(\b+)') and 
                regexp_like(lower(description),  '(\b+)(ear|leg|knee|arm|shoulder|face|head|nose|foot|hand|finger|mouth|neck|wrist|joint|toe)(s){0,1}(\b+)')
                then 1
                else 0 
                end as drop_record
from cdr_nvs001.step5_description
)
where drop_record=0


CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_description_2(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    description_1 STRING, description_2 STRING, description_3 STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_description_2/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')





/*------------------------------------------------
 key words searching 4: pph
---------------------------------------------------*/

SELECT DISTINCT pp.patientUID,  
	   pph.problemonsetdate         as DiagDate         ,               
	   pph.documentationdate as DocumentationDate,	   
	   pph.targetsitetext, pph.targetsitecode, pph.targetsiteuid, pph.mastertargetsiteuid

from (select distinct a.patientUID, a.patientproblemuid
      from cdr_parquet.patientproblem          a
	  inner join cdr_nvs001.step5_diag_eye     b   /* only include those records withtout eye assigned based on ICD10s */
	  on a.patientUID=b.patientUID
	  where b.DiagEYE not in ('1','2','3')  ) pp
inner join cdr_parquet.patientproblemhistory pph
on pp.patientproblemuid = pph.patientproblemuid
where      regexp_like(lower(pph.targetsitetext),      '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000|os|lt|e1|e2|left|le|362503005|ou|50|bilat|both|362508001|244486005)(\b+)')
		OR regexp_like(lower(pph.targetsitecode),      '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000|os|lt|e1|e2|left|le|362503005|ou|50|bilat|both|362508001|244486005)(\b+)')
		OR regexp_like(lower(pph.targetsiteuid),       '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000|os|lt|e1|e2|left|le|362503005|ou|50|bilat|both|362508001|244486005)(\b+)')		
		OR regexp_like(lower(pph.mastertargetsiteuid), '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000|os|lt|e1|e2|left|le|362503005|ou|50|bilat|both|362508001|244486005)(\b+)')



CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_pph(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    targetsitetext STRING, targetsitecode STRING, targetsiteuid STRING, mastertargetsiteuid STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_pph/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')




select DISTINCT patientUID, DiagDate, DocumentationDate,
CASE WHEN drop_record_1=0 and regexp_like(lower(targetsitetext),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000)(\b+)') then 1 else 0 end as targetsitetext_1,
CASE WHEN drop_record_1=0 and regexp_like(lower(targetsitetext),  '(\b+)(no ){0}(os|lt|e1|e2|left|le|362503005)(\b+)') then 1 else 0 end as targetsitetext_2,
CASE WHEN drop_record_1=0 and regexp_like(lower(targetsitetext),  '(\b+)(no ){0}(ou|50|bilat|both|362508001|244486005)(\b+)') then 1 else 0 end as targetsitetext_3,

CASE WHEN drop_record_2=0 and regexp_like(lower(targetsitecode),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000)(\b+)') then 1 else 0 end as targetsitecode_1,
CASE WHEN drop_record_2=0 and regexp_like(lower(targetsitecode),  '(\b+)(no ){0}(os|lt|e1|e2|left|le|362503005)(\b+)') then 1 else 0 end as targetsitecode_2,
CASE WHEN drop_record_2=0 and regexp_like(lower(targetsitecode),  '(\b+)(no ){0}(ou|50|bilat|both|362508001|244486005)(\b+)') then 1 else 0 end as targetsitecode_3,

CASE WHEN drop_record_3=0 and regexp_like(lower(targetsiteuid),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000)(\b+)') then 1 else 0 end as targetsiteuid_1,
CASE WHEN drop_record_3=0 and regexp_like(lower(targetsiteuid),  '(\b+)(no ){0}(os|lt|e1|e2|left|le|362503005)(\b+)') then 1 else 0 end as targetsiteuid_2,
CASE WHEN drop_record_3=0 and regexp_like(lower(targetsiteuid),  '(\b+)(no ){0}(ou|50|bilat|both|362508001|244486005)(\b+)') then 1 else 0 end as targetsiteuid_3,

CASE WHEN drop_record_4=0 and regexp_like(lower(mastertargetsiteuid),  '(\b+)(no ){0}(od|rt|e3|e4|right|re|362502000)(\b+)') then 1 else 0 end as mastertargetsiteuid_1,
CASE WHEN drop_record_4=0 and regexp_like(lower(mastertargetsiteuid),  '(\b+)(no ){0}(os|lt|e1|e2|left|le|362503005)(\b+)') then 1 else 0 end as mastertargetsiteuid_2,
CASE WHEN drop_record_4=0 and regexp_like(lower(mastertargetsiteuid),  '(\b+)(no ){0}(ou|50|bilat|both|362508001|244486005)(\b+)') then 1 else 0 end as mastertargetsiteuid_3

from 
(
SELECT *,
      case when regexp_like(lower(targetsitetext),  '(\b+)(no ){0}(right|left|both)(\b+)') and 
                not regexp_like(lower(targetsitetext),  '(\b+)(eye|eyes)(\b+)') and 
                not regexp_like(lower(targetsitetext),  '(\b+)(od|rt|e3|e4|re|362502000|os|lt|e1|e2|le|362503005|ou|50|bilat|362508001|244486005)(\b+)') and 
                regexp_like(lower(targetsitetext),  '(\b+)(ear|leg|knee|arm|shoulder|face|head|nose|foot|hand|finger|mouth|neck|wrist|joint|toe)(s){0,1}(\b+)')
                then 1 else 0  end as drop_record_1,
                
     case when regexp_like(lower(targetsitecode),  '(\b+)(no ){0}(right|left|both)(\b+)') and 
                not regexp_like(lower(targetsitecode),  '(\b+)(eye|eyes)(\b+)') and 
                not regexp_like(lower(targetsitecode),  '(\b+)(od|rt|e3|e4|re|362502000|os|lt|e1|e2|le|362503005|ou|50|bilat|362508001|244486005)(\b+)') and 
                regexp_like(lower(targetsitecode),  '(\b+)(ear|leg|knee|arm|shoulder|face|head|nose|foot|hand|finger|mouth|neck|wrist|joint|toe)(s){0,1}(\b+)')
                then 1 else 0  end as drop_record_2,

     case when regexp_like(lower(targetsiteuid),  '(\b+)(no ){0}(right|left|both)(\b+)') and 
                not regexp_like(lower(targetsiteuid),  '(\b+)(eye|eyes)(\b+)') and 
                not regexp_like(lower(targetsiteuid),  '(\b+)(od|rt|e3|e4|re|362502000|os|lt|e1|e2|le|362503005|ou|50|bilat|362508001|244486005)(\b+)') and 
                regexp_like(lower(targetsiteuid),  '(\b+)(ear|leg|knee|arm|shoulder|face|head|nose|foot|hand|finger|mouth|neck|wrist|joint|toe)(s){0,1}(\b+)')
                then 1 else 0  end as drop_record_3,

     case when regexp_like(lower(mastertargetsiteuid),  '(\b+)(no ){0}(right|left|both)(\b+)') and 
                not regexp_like(lower(mastertargetsiteuid),  '(\b+)(eye|eyes)(\b+)') and 
                not regexp_like(lower(mastertargetsiteuid),  '(\b+)(od|rt|e3|e4|re|362502000|os|lt|e1|e2|le|362503005|ou|50|bilat|362508001|244486005)(\b+)') and 
                regexp_like(lower(mastertargetsiteuid),  '(\b+)(ear|leg|knee|arm|shoulder|face|head|nose|foot|hand|finger|mouth|neck|wrist|joint|toe)(s){0,1}(\b+)')
                then 1 else 0  end as drop_record_4
                
from cdr_nvs001.step5_pph
)
where drop_record_1=0 or drop_record_2=0 or drop_record_3=0 or drop_record_4=0




CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_pph_2(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    targetsitetext_1 STRING, targetsitetext_2 STRING, targetsitetext_3 STRING,
    targetsitecode_1  STRING, targetsitecode_2  STRING, targetsitecode_3  STRING, 
    targetsiteuid_1  STRING, targetsiteuid_2  STRING, targetsiteuid_3  STRING,
    mastertargetsiteuid_1  STRING, mastertargetsiteuid_2  STRING, mastertargetsiteuid_3  STRING   
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_pph_2/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')
 
 


/*------------------------------------------------
 create final eye columns based on all key words searching
---------------------------------------------------*/


select distinct coalesce(pp.patientUID, pph.patientUID) as patientUID,
				   coalesce(pp.DiagDate, pph.DiagDate) as DiagDate,
				   coalesce(pp.DocumentationDate, pph.DocumentationDate) as DocumentationDate,
				   case when pph.right_eye_pph='1' or pp.right_eye_2='1' then '1' else '0' end as right_eye,
				   case when pph.left_eye_pph='1' or pp.left_eye_2='1'  then '1' else '0' end as left_eye,
				   case when pph.both_eyes_pph='1' or pp.both_eyes_2='1' then '1' else '0' end as both_eyes
from 
(
		select distinct coalesce(c.patientUID, d.patientUID) as patientUID,
				   coalesce(c.DiagDate, d.DiagDate) as DiagDate,
				   coalesce(c.DocumentationDate, d.DocumentationDate) as DocumentationDate,
				   case when d.description_1='1' or c.right_eye_1='1' then '1' else '0' end as right_eye_2,
				   case when d.description_2='1' or c.left_eye_1='1'  then '1' else '0' end as left_eye_2,
				   case when d.description_3='1' or c.both_eyes_1='1' then '1' else '0' end as both_eyes_2
		from 
			(
				select distinct coalesce(a.patientUID, b.patientUID) as patientUID,
					   coalesce(a.DiagDate, b.DiagDate) as DiagDate,
					   coalesce(a.DocumentationDate, b.DocumentationDate) as DocumentationDate,
					   case when a.practicedescription_1='1' or b.problemcomment_1='1' then '1' else '0' end as right_eye_1,
					   case when a.practicedescription_2='1' or b.problemcomment_2='1' then '1' else '0' end as left_eye_1,
					   case when a.practicedescription_3='1' or b.problemcomment_3='1' then '1' else '0' end as both_eyes_1
		   
				from (select * from cdr_nvs001.step5_practicedes_2  where practicedescription_1='1' or practicedescription_2='1' or practicedescription_3='1'  )     a  
				FULL JOIN (select * from cdr_nvs001.step5_problemcomment_2 where problemcomment_1='1' or problemcomment_2='1' or problemcomment_3='1'  )             b
				on a.patientUID=b.patientUID and a.DiagDate=b.DiagDate and a.DocumentationDate=b.DocumentationDate
			)  c
		FULL JOIN (select * from cdr_nvs001.step5_description_2  where description_1='1' or description_2='1' or description_3='1'  )  d
		on c.patientUID=d.patientUID and c.DiagDate=d.DiagDate and c.DocumentationDate=d.DocumentationDate
)  pp
FULL JOIN 
(select  distinct patientUID, DiagDate, DocumentationDate,
       case when targetsitetext_1='1' or targetsitecode_1='1' or targetsiteuid_1='1' or mastertargetsiteuid_1='1' then '1' else '0' end as right_eye_pph,
       case when targetsitetext_2='1' or targetsitecode_2='1' or targetsiteuid_2='1' or mastertargetsiteuid_2='1' then '1' else '0' end as left_eye_pph,
       case when targetsitetext_3='1' or targetsitecode_3='1' or targetsiteuid_3='1' or mastertargetsiteuid_3='1' then '1' else '0' end as both_eyes_pph       
from cdr_nvs001.step5_pph_2) pph
on pp.patientUID=pph.patientUID and pp.DiagDate=pph.DiagDate and pp.DocumentationDate=pph.DocumentationDate




CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_diag_eye_combine(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    right_eye  STRING, left_eye  STRING, both_eyes STRING   
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_diag_eye_combine/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')


/* frequency of diag_eye*/
select right_eye, left_eye, both_eyes, count(*) as count
from cdr_nvs001.step5_diag_eye_combine
group by right_eye, left_eye, both_eyes


/* Recode bilateral eyes to 1 AND 2 */

SELECT distinct *
FROM 
(
select distinct patientUID, DiagDate, DocumentationDate,
       '1' AS dxeye
from cdr_nvs001.step5_diag_eye_combine
where both_eyes='1'

UNION
select distinct patientUID, DiagDate, DocumentationDate,
       '2' AS dxeye
from cdr_nvs001.step5_diag_eye_combine
where both_eyes='1'

UNION
select distinct patientUID, DiagDate, DocumentationDate,
       '1' AS dxeye
from cdr_nvs001.step5_diag_eye_combine
where right_eye='1' and left_eye='0' and both_eyes='0'

UNION
select distinct patientUID, DiagDate, DocumentationDate,
       '2' AS dxeye
from cdr_nvs001.step5_diag_eye_combine
where right_eye='0' and left_eye='1' and both_eyes='0'
)



CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_diag_eye_combine_2(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    dxeye STRING   
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_diag_eye_combine_2/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')



/*------------------------------------------------
 union two parts of diagEYE: 
 (a) created based on ICD10 (n = 15,813,401)
 (b) created based on keywords searching for those records without ICD10 eyes assigned (n = 43,930,903)
---------------------------------------------------*/

SELECT DISTINCT *
FROM
(
	/* (a) created based on ICD10 (n = 15,813,401) */

	select distinct patientUID, DiagDate, DocumentationDate, '1' as dxeye,
		   nAMD, macular_edema, DME, diabetic_ret, MCN
	from cdr_nvs001.step5_diag_eye            
	where DiagEYE = '1' 

	UNION
	select distinct patientUID, DiagDate, DocumentationDate, '2' as dxeye,
		   nAMD, macular_edema, DME, diabetic_ret, MCN
	from cdr_nvs001.step5_diag_eye            
	where DiagEYE = '2'

	UNION
	select distinct patientUID, DiagDate, DocumentationDate, '1' as dxeye,
		   nAMD, macular_edema, DME, diabetic_ret, MCN
	from cdr_nvs001.step5_diag_eye            
	where DiagEYE = '3' 

	UNION
	select distinct patientUID, DiagDate, DocumentationDate, '2' as dxeye,
		   nAMD, macular_edema, DME, diabetic_ret, MCN
	from cdr_nvs001.step5_diag_eye            
	where DiagEYE = '3'

	/* (b) created based on keywords searching for those records without ICD10 eyes assigned (n = 43,930,903) */

	UNION
	select distinct a.patientUID, a.DiagDate, a.DocumentationDate, a.dxeye,
		   b.nAMD, b.macular_edema, b.DME, b.diabetic_ret, b.MCN
	from  cdr_nvs001.step5_diag_eye_combine_2       a
	inner join cdr_nvs001.step5_diag_eye            b
	on a.patientUID=b.patientUID and a.DiagDate=b.DiagDate and a.DocumentationDate=b.DocumentationDate
	where b.DiagEYE not in ('1','2','3') 
)



CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_diag_eye_final(
    patientUID STRING, DiagDate STRING, DocumentationDate STRING,
    dxeye STRING, nAMD STRING, macular_edema STRING, DME STRING, diabetic_ret STRING, MCN  STRING  
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_diag_eye_final/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')
 
 /* 52,870,026 */
 select count(*) as count from cdr_nvs001.step5_diag_eye_final
 
 
 
 
 /* 52,600,225 */
select distinct b.verana_temp_guid, a.DiagDate, a.DocumentationDate, a.dxeye,
	   a.nAMD, a.macular_edema, a.DME, a.diabetic_ret, a.MCN 
from           cdr_nvs001.step5_diag_eye_final   a
inner join     cdr_nvs001.guid_map               b
on a.patientUID=b.patientUID



 
CREATE EXTERNAL TABLE IF NOT EXISTS cdr_nvs001.step5_diag_eye_final_2(
    verana_temp_guid STRING, DiagDate STRING, DocumentationDate STRING,
    dxeye STRING, nAMD STRING, macular_edema STRING, DME STRING, diabetic_ret STRING, MCN  STRING  
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
 LOCATION  's3://fillmore-prod/athena-query-results/us-east-1/A-XB/nvs_step5_diag_eye_final_2/2018/09/18'
 TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count' = '1')
 
 
 
 
 
 
 


