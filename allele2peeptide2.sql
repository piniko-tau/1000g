

select peptid_group from (

	select gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (

		select distinct gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (

			select gene_name,rsid,$patient_snp_variation, 

				case  

					when substr($patient_snp_variation,1,1) = allele1 then '+'||peptide1 
					when substr($patient_snp_variation,1,1) = allele2 then '+'||peptide2 
					when substr($patient_snp_variation,1,1) = allele3 then '+'||peptide3 
					when substr($patient_snp_variation,1,1) = opallele1 then '-'||peptide1 
					when substr($patient_snp_variation,1,1) = opallele2 then '-'||peptide2 
					when substr($patient_snp_variation,1,1) = opallele3 then '-'||peptide3 

				end as t1 , 

				case 
				
					when substr($patient_snp_variation,3,1) = allele1 then '+'||peptide1 
					when substr($patient_snp_variation,3,1) = allele2 then '+'||peptide2 
					when substr($patient_snp_variation,3,1) = allele3 then '+'||peptide3 
					when substr($patient_snp_variation,3,1) = opallele1 then '-'||peptide1 
					when substr($patient_snp_variation,3,1) = opallele2 then '-'||peptide2 
					when substr($patient_snp_variation,3,1) = opallele3 then '-'||peptide3 

				end as t2 ,

				case 

					when substr($patient_snp_variation,5,1) = '' then '' 
					when substr($patient_snp_variation,5,1) = allele1 then '+'||peptide1 
					when substr($patient_snp_variation,5,1) = allele2 then '+'||peptide2 
					when substr($patient_snp_variation,5,1) = allele3 then '+'||peptide3 
					when substr($patient_snp_variation,5,1) = opallele1 then '-'||peptide1 
					when substr($patient_snp_variation,5,1) = opallele2 then '-'||peptide2 
					when substr($patient_snp_variation,5,1) = opallele3 then '-'||peptide3 

				end as t3 

				from  $patient_snp_variation) as mytable )as t1 group by t1.gene_name order by t1.gene_name) as t5;


check :  ECI1 | rs2302584 rs26840 | QUERCETINantioxidants is -I-I+R+R


























#############################################################################################################################



select ap,peptid_group from (select string_agg(ap,',') as ap,gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (select distinct ap,gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (select szpabr0002||'-'||alleles||'-'||peptides as ap,gene_name,rsid,szpabr0002, case when substr(szpabr0002,1,1) = allele1 then '+'||peptide1 when substr(szpabr0002,1,1) = allele2 then '+'||peptide2 when substr(szpabr0002,1,1) = allele3 then '+'||peptide3 when substr(szpabr0002,1,1) = opallele1 then '-'||peptide1 when substr(szpabr0002,1,1) = opallele2 then '-'||peptide2 when substr(szpabr0002,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(szpabr0002,3,1) = allele1 then '+'||peptide1 when substr(szpabr0002,3,1) = allele2 then '+'||peptide2 when substr(szpabr0002,3,1) = allele3 then '+'||peptide3 when substr(szpabr0002,3,1) = opallele1 then '-'||peptide1 when substr(szpabr0002,3,1) = opallele2 then '-'||peptide2 when substr(szpabr0002,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(szpabr0002,5,1) = '' then '' when substr(szpabr0002,5,1) = allele1 then '+'||peptide1 when substr(szpabr0002,5,1) = allele2 then '+'||peptide2 when substr(szpabr0002,5,1) = allele3 then '+'||peptide3 when substr(szpabr0002,5,1) = opallele1 then '-'||peptide1 when substr(szpabr0002,5,1) = opallele2 then '-'||peptide2 when substr(szpabr0002,5,1) = opallele3 then '-'||peptide3 end as t3 from  mind_data_1_rs_ensorted_by_gene_posann) as mytable )as t4 group by t4.gene_name order by t4.gene_name) as t5 limit 5;

select ap,peptid_group from (

	select string_agg(ap,',') as ap,gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (

		select distinct ap,gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (

select szpabr0002||'-'||alleles||'-'||peptides as ap,gene_name,rsid,szpabr0002, case when substr(szpabr0002,1,1) = allele1 then '+'||peptide1 when substr(szpabr0002,1,1) = allele2 then '+'||peptide2 when substr(szpabr0002,1,1) = allele3 then '+'||peptide3 when substr(szpabr0002,1,1) = opallele1 then '-'||peptide1 when substr(szpabr0002,1,1) = opallele2 then '-'||peptide2 when substr(szpabr0002,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(szpabr0002,3,1) = allele1 then '+'||peptide1 when substr(szpabr0002,3,1) = allele2 then '+'||peptide2 when substr(szpabr0002,3,1) = allele3 then '+'||peptide3 when substr(szpabr0002,3,1) = opallele1 then '-'||peptide1 when substr(szpabr0002,3,1) = opallele2 then '-'||peptide2 when substr(szpabr0002,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(szpabr0002,5,1) = '' then '' when substr(szpabr0002,5,1) = allele1 then '+'||peptide1 when substr(szpabr0002,5,1) = allele2 then '+'||peptide2 when substr(szpabr0002,5,1) = allele3 then '+'||peptide3 when substr(szpabr0002,5,1) = opallele1 then '-'||peptide1 when substr(szpabr0002,5,1) = opallele2 then '-'||peptide2 when substr(szpabr0002,5,1) = opallele3 then '-'||peptide3 end as t3 from  mind_data_1_rs_ensorted_by_gene_posann) as mytable )as t4 group by t4.gene_name order by t4.gene_name) as t5 limit 5;

