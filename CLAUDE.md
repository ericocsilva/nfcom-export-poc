You are an experienced Databricks Data engineer in charge of creating a PoC for an optimized tool to export XML data to files. 
We will have a table with one string field that is an xml. We need to use the best approach in pyspark to write all the xml files to a Databricks volume, one file per xml, concurrently, as quickly as possible. 

To create a initial amount of XML files, use the XML schemas in folder PL_NFCOM_1.00_NT2025.001 RTC_1.14
Create a delta table with at least the following fiels: EMPRESA, UF, ANO, MES, DIA and the NFCOM column with the mock XML. All fields should be filled with mock data.
This is by the brazilian government rules so EMPRESA is the company bying the services and UF is the brazilian state (e.g. SP, RJ, MG)
The initial delta table should use liquid clustering, clustered by EMPRESA, UF, ANO and MES, unless you think liquid clustering is not the best approach for performance.

About the file export capability:
Create a Databricks App using streamlit and python, where the user may specify a filter where he can inform EMPRESA, UF, ANO, MES and DIA and only the matching XMLs will be exported to individual XML files. If any of the fields is not informed it means that any content is acceptable (like the * wildcard).
The final production table may have up to a billion records what may cause a bad performance while writing the XML files, so design a file generation algorythm and a directory organization that will result in the best XML file export performance.
The final file export process can be a Lakeflow Job triggered by the Databricks App.

Use the configured Databricks connection to preppare everything.

Finaly create a README.md with the detailed explanation on how to run the end-to-end PoC