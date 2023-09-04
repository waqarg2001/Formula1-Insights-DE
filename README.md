<p align='center'>
<img src='https://github.com/waqarg2001/Formula1-Insights-DE/blob/master/resources/image.png' width=600 height=250 >
</p>

---

<h4 align='center'> Application of ETL process on raw used cars dataset scraped from <a href='https://pakwheels.com/' target='_blank'>PakWheels</a> along with its analysis using <a href='jupyter.org' target='_blank'>Jupyter</a> Notebook. </h4>

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

<p><a href='pakwheels.com' target='_blank'>PakWheels</a> is the largest online marketplace for car shoppers and sellers in Pakistan. It aggregates thousands of new, used, and certified second-hand cars from thousands of dealers and private sellers.</p>

This project involves Extract Transform Load(ETL) process on used and new cars dataset which was scraped from PakWheels . Exploratory Data Analysis(EDA) is performed on it using Jupyter Notebook to extract key insights about the Pakistan's used cars marketplace.

The repository directory structure is as follows:

```
├── LICENSE 
├── README.md          <- The top-level README for developers using this project. 
| 
├── run.py             <- Python script to start ETL process. 
| 
├── data 
│   ├── processed      <- The final, canonical data set for analysis.
│   └── raw            <- The original, immutable data dump. 
│ 
│ 
│ 
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-mwg-initial-data-exploration`.  
| 
│ 
├── src                <- Source code for use in this project. 
│   ├── __init__.py    <- Makes src a Python module. 
│   │ 
│   ├── data           <- Script to perform ETL. 
│       └── make_dataset.py 
|         
|
├── resources          <- Resources for this readme file. 
```

## Tools 

To build this project, following tools were used:

- Python
- PyCharm
- Github
- Jupyter Notebook

## Architecture

The architecture of this project is straightforward which can be understood by the following diagram.

<p align='center'>
  <img src='https://github.com/waqarg2001/PakWheels-Data-Analysis/blob/7b23ca6ab3df0c13053a73f5a91e5544becd2ff0/resources/architecture.gif' height=280 width=900>
</p>  

According to the diagram we first create a python script which performs ETL for us on the raw dataset. The output of this process is clean data which is then used for exploratory analysis in Jupyter Notebook.


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
