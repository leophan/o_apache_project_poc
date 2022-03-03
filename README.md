## READ ME

### Download code
```shell
git clone 
```

### Setup
```shell
pip install virtualenv
# cd project_folder
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Submit job
```bin/spark-submit \
    --master spark://LEO-PC.localdomain:7077 \
    --py-files ./o_apache_project_poc/src/utils.py \
    ./o_apache_project_poc/src/main.py
   