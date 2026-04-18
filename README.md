# etl-project-second-delivery
check first delivery link: https://github.com/juandavidmeza-prog/etl-project-first-delivery/tree/main

## Team Members
$$
\begin{array}{|c|c|}
\hline
\text{Código} & \text{Apellido} & \text{Nombre}\\
\hline
\text {2240129}  & \text {Meza}  & \text {Juan David} \\
\text {2243533}  & \text {Uribe}  & \text {Santiago}  \\
\text {2249266}  & \text {Zambrano}  & \text {Andrés}  \\
\hline
\end{array}
$$

## Libraries
The project was designed to run on the Google Colab environment. While this poses some limitations to the scope and availability of the notebook and its dependencies, the pros outweighted the cons. Namely, the guarantee that it would run under the preset configuration for every team member or evaluator, as well as the comfort of an option that was already tried and tested along the course's activities. 

Even though no dependencies are to be installed locally, the notebook does ask for a few imports. Their names and roles are as presented: 

* Pandas: dataframe management, EDA, plotly, and `.csv` to `.db` intermediary.  
* SQLite3: python + SQL. Replaced DuckDB as per the Airflow setup provided during the course.  
* plotly: most competent way of integrating a dashboard visualization into Google Colab.
* Airflow and Great Expectations: orchestrator and data quality validator for the pipeline, respectively, in accordance to the expected scope of the project. Worth noting, their use and implementation is identical to the `airflow_gx.ipynb` example displayed during class.  
* Requests: handling OpenAQ and REST Countries respective API calls.  
* json: API responses handling commodity, as they are sent in `.json` format, meaning the tailored library is rather suitable.  
* os, datetime: emotional support.  

## Technologies 
### Data Warehouse Architecture 
Succeeding the previous submission, the team focused on expanding the scope of information the warehouse could provide. To this extent, it was decided OpenAQ would fit the job, as it offers valuable, real, and updated insights on the different countries' air conditions (when the API responses work, that is). Given standarized nomenclature is a top priority, the third source that should also guarantee coherence on this regard, and ideally elaborate on the available information for each data point. [REST Countries](https://restcountries.com/) was ultimately chosen for the task, although scrapping [Wikipedia's ISO 3166-1 page](https://en.wikipedia.org/wiki/ISO_3166-1) (or its variants) was considered for the same end.  

The facts remain the same (not literally though): it is still a star schema, albeit with a new fact table (provided by OpenAQ) and a small addition to the location dimension (REST Countries) --and the relevant join, of course.  


```mermaid
erDiagram
    FACT_CLEAN_FUEL_ACCESS {
        string location_id
        int time_id
        string residence_id
        string indicator_id
        float value
        float lower_bound
        float upper_bound
        boolean is_latest_year
    }

    FACT_AIR_QUALITY {
        string iso_alpha3
        float pm25_value
        float clean_fuel_access
        datetime timestamp
    }

    DIM_LOCATION {
        string iso_alpha3 PK
        string country_name
        string region
        int population
    }

    DIM_TIME {
        int time_id
        int year
        int decade
    }

    DIM_RESIDENCE {
        string residence_id
        string residence_type
    }

    DIM_INDICATOR {
        string indicator_id
        string indicator_name
        string unit
    }

    FACT_AIR_QUALITY }o--|| DIM_LOCATION : "joins on iso_alpha3"
    FACT_CLEAN_FUEL_ACCESS }o--|| DIM_LOCATION : joins
    FACT_CLEAN_FUEL_ACCESS }o--|| DIM_TIME : joins
    FACT_CLEAN_FUEL_ACCESS }o--|| DIM_RESIDENCE : joins
    FACT_CLEAN_FUEL_ACCESS }o--|| DIM_INDICATOR : joins
```


### Airflow DAG design
Once again emulating the course's demonstration, the team designed the DAG such as it ingests the data and then validates it before loading it into the final warehouse. While it could be separated into three different branches (i.e., each source being processed on its own and having the validation apply individually), the team decided it was not the effort given the reliability of the sources: one is unchanging (WHO), one is big enough to pretty much guarantee it will be available 24/7 (REST Countries), and one is a small opensource project with ~~barely a few hundred people network and a distinctive lack of consistency in their API to the point we might as well treat it as nonexistent~~ limited support and interruptions in their service status (OpenAQ). Under this optic, it was deemed more realistic to treat the whole pipeline as a consistent stream that would only significantly break down under abnormal circumstances, and otherwise deprecate the unresponsive parts and either replace them manually or substitute them with a dummy value until a more reliable, consistent solucion is found.   

```mermaid
flowchart TD
    A[extract_transform_task] --> B[validate_task]

    B --> C[load_dw_task]
    C --> F[fin]

    B --> D[mover_cuarentena]
    D --> E[enviar_alerta]
    E --> F
```

### Dashboard (click me)
[![countries of the world plot with air quality info](https://raw.githubusercontent.com/juandavidmeza-prog/etl-project-first-delivery/refs/heads/second_delivery/resources/countriesplot.png)](https://azambrano25.github.io/etl-embeds/plotly_example.html)
