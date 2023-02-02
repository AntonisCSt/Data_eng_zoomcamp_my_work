if you want to run terraform make sure GOOGLE_APPLICATION_CREDENTIALS got the json

`run echo $GOOGLE_APPLICATION_CREDENTIALS`
`echo $?`

if 1 then run

`export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/my-rides-antonis-73c80e59a497.json`



## register blocks for prefect and gcp

`prefect block register -m prefect_gcp`



