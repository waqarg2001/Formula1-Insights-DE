<p align='center'>
<img src='https://github.com/waqarg2001/Formula1-Insights-DE/blob/master/resources/image.png' width=600 height=300 >
</p>

---

<h4 align='center'> Utilisation of <a href='https://azure.microsoft.com/en-us' target='_blank'>Azure Cloud Services</a> to architect and orchestrate data pipeline to perform ETL on Formula 1 racing dataset extracted from <a href='https://ergast.com/mrd/'>Ergast Developer API.</a> </h4>

<p align='center'>
<img src="https://i.ibb.co/KxfMMsP/built-with-love.png" alt="built-with-love" border="0">
<img src="https://i.ibb.co/MBDK1Pk/powered-by-coffee.png" alt="powered-by-coffee" border="0">
<img src="https://i.ibb.co/CtGqhQH/cc-nc-sa.png" alt="cc-nc-sa" border="0">
</p>

<p align="center">
  <a href="#overview">Overview</a> •
  <a href="#tools">Tools</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#demo">Demo</a> •
  <a href="#support">Support</a> •
  <a href="#license">License</a>
</p>


## Overview

<p>The Ergast Developer API is an experimental web service that provides a historical record of motor racing data for non-commercial purposes. The API provides data for the Formula One series, from the beginning of the world championships in 1950 until now.</p>

This project showcases a seamless data journey facilitated by Azure services. It begins with data extraction from the Ergast Developer API and harnesses Azure components such as Azure Active Directory, Service Principal, Azure Databricks, Key Vault, Azure Data Factory, and Azure Data Lake Gen2 to orchestrate this process efficiently. Within Azure Databricks, powered by Apache Spark, data undergoes the ETL (Extract, Transform, Load) process. The data begins its journey in the 'ingestion' folder, where it is initially received. It then proceeds to the 'transformations' folder, where it is refined and enhanced. Finally, the data finds its destination in the 'analysis' folder, where it is carefully organized and prepared for analysis. The orchestration of this data journey is managed through Azure Data Factory, representing a structured and efficient approach to data engineering and analysis.

The repository directory structure is as follows:

```
├── README.md          <- The top-level README for developers using this project. 
| 
├── Raw           <- Contains script to define table schemas
| 
├── Transformations         <- Scripts to aggregate and transform data
│  
│ 
├── analysis         <- Basic analysis of data from the transformations folder.  
| 
│ 
├── include                <- Configuration folder 
│   ├── common_functions.py    <- Common functions used throughout the ETL process.
│   │ 
│   ├── configuration.py       <- Houses configuration settings such as variables.
│      
|         
|
├── ingestion          <- Ingestion scripts for data files from ADLS Gen 2.
│      
├── resources          <- Resources for readme file.
|
├── set-up             <- Script for mounting ADLS Gen 2 to Databricks
|         
├── utils              <- SQL scripts for incremental load.
```

## Tools 

To build this project, the following tools were used:

- Azure Databricks
- Azure KeyVault
- Azure Active Directory
- Azure DataLake Gen 2
- Azure Data Factory
- Pyspark
- SQL
- Git

## Architecture

The architecture of this project is inspired by the following, taken from Azure Architecture Center.

<p align='center'>
  <img src='https://github.com/waqarg2001/PakWheels-Data-Analysis/blob/7b23ca6ab3df0c13053a73f5a91e5544becd2ff0/resources/architecture.gif' height=280 width=900>
</p>  

According to the diagram we first create a python script that performs ETL for us on the raw dataset. The output of this process is clean data which is then used for exploratory analysis in Jupyter Notebook.


## Demo

The figure below shows a snapshot of ETL process being conducted through terminal. Type run.py (raw data directory).
(figure may take few seconds to load)

<p>
  <img src='https://github.com/waqarg2001/PakWheels-Data-Analysis/blob/b195a77dc208fe9c668df46433f213108ae63008/resources/pakwheels%20etl.gif' width=900 height=300>
</p>  




## Support

If you have any doubts, queries or, suggestions then, please connect with me on any of the following platforms:

[![Linkedin Badge][linkedinbadge]][linkedin] 
[![Gmail Badge][gmailbadge]][gmail]


## License

<a href = 'https://creativecommons.org/licenses/by-nc-sa/4.0/' target="_blank">
    <img src="https://i.ibb.co/mvmWGkm/by-nc-sa.png" alt="by-nc-sa" border="0" width="88" height="31">
</a>

This license allows reusers to distribute, remix, adapt, and build upon the material in any medium or format for noncommercial purposes only, and only so long as attribution is given to the creator. If you remix, adapt, or build upon the material, you must license the modified material under identical terms.



<!--Profile Link-->
[linkedin]: https://www.linkedin.com/in/waqargul
[gmail]: mailto:waqargul6@gmail.com

<!--Logo Link -->
[linkedinbadge]: https://img.shields.io/badge/waqargul-0077B5?style=for-the-badge&logo=linkedin&logoColor=white
[gmailbadge]: https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white
