### Creating new env for prefect week 2
`conda create -n zoomcamp_week_2 python=3.9`
`conda activate zooomcamp_week_2` 
`pip install requirements.txt`
`pip install docker-requirements.txt`

### starting orion UI
`prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`
`prefect orion start`