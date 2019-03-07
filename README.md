Assaf Http Server
==================

High Performance Frontend

### Build docker image and push it
```bash
make build
```

### Test
```bash
make test
```

### Run

make sure this variables are availibe on your enviroment:
```
export rabbitmq_host=localhost
export rabbitmq_port=5672
export rabbitmq_username=guest
export rabbitmq_password=guest
```
you can put these in a file and source it like

```
. env_file
``` 
then run assaf-server
```
build/assaf-server
```

