import ipywidgets as widgets
import pandas as pd
from ipywidgets import Button, Layout
from IPython.display import display
import re, json, datetime, argparse, sys, os, ast, shutil
import nbformat as nbf
from Fast_connectCloud import connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_platform import connect_cloud_platform


def generate_notebook_processing(params):
    """
    """

    notebook_name = params['notebook_name']
    project_name = params['project_name']
    input_datasets = params['input_datasets']
    sheetnames = params['sheetnames']
    bigquery_dataset = params['bigquery_dataset']
    destination_engine = params['destination_engine']
    path_ = params['path_processing']
    path_notebook = params['path_notebook_processing']
    project='valid-pagoda-132423'
    username = "thomas"

    with open(path_notebook) as f:
        ipynb = json.load(f)

    nb_cells = []
    output_str = ""

# Parse cells in Jupyter notebook template
    for cell in ipynb['cells']:
        cell_type = cell['cell_type']
        source = cell['source']
        nb_cells.append([cell_type, source])

## Change header
    nb_cells[0][1][0] = nb_cells[0][1][0].replace('TITLE', notebook_name)
    now = datetime.datetime.now()
    nb_cells[0][1][2] = nb_cells[0][1][2].replace('XX',
     now.strftime("%Y-%m-%d %H:%M %Z"))

### Add Project name

    nb_cells[5][1][8] = nb_cells[5][1][8].replace('PROJECTNAME', project_name) ### Pagoda
    nb_cells[-5][1][4]= nb_cells[-5][1][4].replace('PROJECTNAME', project_name)

##### Find if prefix in list of dtaset
###### If dataset found, then remove from list, and make
##### special variable for special treatment
##### So far only one preffix dataset can be added

    regex = r"\*"
    for d in input_datasets:
        matches = re.search(regex, d)
        if matches:
            dataset_suffix = d.replace("*", "")
            input_datasets.remove(d)
        else:
            try:
                if dataset_suffix:
                    pass
            except:
                dataset_suffix = False

## Add data source
## Load dataset
    datasets = gcp.list_dataset()
    bq_contents = []
### To recover the index of the sheetname in sheetname

    for key, value in datasets.items():
        for dataset in value:
            tables = gcp.list_tables(dataset = dataset)
            bq_contents.append(tables)
## Load Buckets
    buckets = gcp.list_bucket()
    gcs_contents = []
    for key, value in buckets.items():
        for bucket in value:
            blob = gcp.list_blob(bucket = bucket)
            gcs_contents.append(blob)

            dic_result = {
                'BQ': {'Filename': []},
                'GCS': {'Filename': []},
                'GS': {'Filename': [],
                       'ID': [],
                       'sheetname':[]},
            }
            sheetindex = 0

            for l_dataset in input_datasets:
                found = False
                ### Search in BQ
                for i in range(0, len(bq_contents)):
                    regex = r'(\w*{0}\w*)'.format(l_dataset)
                    matches = re.search(regex, str(bq_contents[i]))
                    if matches:
					# print(l_dataset)
                        dic_result['BQ']['Filename'].append(l_dataset)
                        found = True
			### Search in GCS
                if found == False:
                    found = False
                    for i in range(0, len(gcs_contents)):
                        regex = r'(\w*{0}\w*)'.format(l_dataset)
                        matches = re.search(regex, str(gcs_contents[i]))
                        if matches:
                            dic_result['GCS']['Filename'].append(l_dataset)
                            found = True

                if found == False:
                    found = False
					### Search in GS
                    ID_gS = gdr.find_file_id(file_name=l_dataset)

                    if sheetindex < len(sheetnames):
                        sheetName = sheetnames[sheetindex]
                        dic_result['GS']['sheetname'].append(sheetName)
                        dic_result['GS']['Filename'].append(l_dataset)
                        dic_result['GS']['ID'].append(ID_gS)
                        sheetindex += 1

### Add destination

    if destination_engine == 'GCP':
        nb_cells[2][1][5] = nb_cells[2][1][5].replace('None',
												  notebook_name + '.gz')
        nb_cells[2][1][6] = nb_cells[2][1][6].replace('None',
												  notebook_name)
    else:
        nb_cells[2][1][4] = nb_cells[2][1][4].replace('None',
												  notebook_name)

### Add load data
    dic_load_data = {
	'BQ':{'markdown': [],
			'code':[]
		   },
	'GCS': {'markdown': [],
			'code':[]
		   },
	'GS':{'markdown': [],
			'code':[]
		   }
}
    template_gcs_load = """
gcp.download_blob(bucket_name = '{0}',
				  destination_blob_name = '{1}',
				  source_file_name = '{2}')

    df_{3} = pd.read_csv('{4}',
					  compression='gzip',
					  header=0,
					  sep=',',
					  quotechar='"',
					  error_bad_lines=False
					  )
    """

    template_gcs_md = """
## Load {0} from Google Cloud Storage

    Feel free to add description about the dataset or any usefull information.

    """

#### GCS
##### Only one file from  GCS?

    if dic_result['GCS']['Filename']:
        for f, file in enumerate(dic_result['GCS']['Filename']):
            filename = dic_result['GCS']['Filename'][f]
            for p, bucket in enumerate(gcs_contents):
                for i, blob in enumerate(bucket['blob']):
                    regex = r'(\w*{0}\w*)'.format(file)
                    matches = re.search(regex, str(blob))

                    if matches:
                        df_ = file.split(".")[0]
					#path = gcs_contents[p]['blob'][3].replace(
					#'/' + file, '')
					### get path
                        regex = r"/[^/]*$"
                        path = re.sub(regex, "", blob)
                        bucket_name  = bucket['Bucket']
                        ggs_cell = template_gcs_load.format(bucket_name,
										 path,
										 filename,
										 df_,
										 filename
												   )
                        ggs_md = template_gcs_md.format(filename)
                        dic_load_data['GCS']['markdown'].append(ggs_md)
                        dic_load_data['GCS']['code'].append(ggs_cell)

        dic_destination_inf = {
            'Bucket':bucket_name,
            'destination_blob_name':path,
            'new_destination_blob': path.replace('Raw_', 'Processed_')
	}
    else:
	#### We don't know where to save the dataset if no table uploaded from GCS
        dic_destination_inf = {
            'Bucket':'NEED TO DEFINE',
            'destination_blob_name':'NEED TO DEFINE',
            'new_destination_blob':  'XXXXX/Processed_'
	}
##### GBQ
    template_gbq_load = """
query = (
          "SELECT * "
            "FROM {0}.{1} "

        )

df_{1} = gcp.upload_data_from_bigquery(query = query, location = 'US')
df_{1}.head()
    """

    template_gbq_md = """
## Load {0} from Google Big Query

Feel free to add description about the dataset or any usefull information.

    """
    if dic_result['BQ']['Filename']:
        for f, file in enumerate(dic_result['BQ']['Filename']):
            filename = dic_result['BQ']['Filename'][f]
#### Note need to search for thr bigquery dataset. Right now, use the predefined one in the argument
            cell_gbq = template_gbq_load.format(bigquery_dataset[0],
                filename)
            cell_md = template_gbq_md.format(filename)
            dic_load_data['BQ']['markdown'].append(cell_md)
            dic_load_data['BQ']['code'].append(cell_gbq)

##### GS
    template_gs_load = """
### Please go here {0}
### To change the range

sheetid = '{1}'
sheetname = '{2}'

df_{3} = gdr.upload_data_from_spreadsheet(sheetID = sheetid,
sheetName = sheetname,
         to_dataframe = True)
df_{3}.head()
    """

    template_gs_md = """
## Load {0} from Google Spreadsheet

Feel free to add description about the dataset or any usefull information.

Profiling will be available soon for this dataset

    """
    if dic_result['GS']['ID']:
        for f, iD in enumerate(dic_result['GS']['ID']):
            sheetid = iD
            sheetname = dic_result['GS']['sheetname'][f]
            filename = dic_result['GS']['Filename'][f]
            url_gs = 'https://docs.google.com/spreadsheets/d/{0}'
            url_gs = url_gs.format(sheetid)
            cell_gs = template_gs_load.format(url_gs,
									  sheetid,
									  sheetname,
									  filename)

            cell_md = template_gs_md.format(filename)
            dic_load_data['GS']['markdown'].append(cell_md)
            dic_load_data['GS']['code'].append(cell_gs)

##### Special treatment for prefix
#### 1 find path in GCS with dot. It means the element in
#### gcs_contents is a filename
#### 2 get the path of the blob, ie remove filename from path
#### 3 Drop duplicate path

    if dataset_suffix:
        regex_dot = r"[.]"
        regex_slash = r".*(?=/.)"
        list_blob_dot = []
        for bucket in gcs_contents:
            for blob in bucket['blob']:
                matches = re.search(regex_dot, blob)
                if matches:
                    matches_ = re.search(regex_slash, blob)
                    full_path = '{0}/{1}'
                    full_path = full_path.format(bucket['Bucket'], matches_.group())
                    list_blob_dot.append(full_path)
        list_blob_dot = list(dict.fromkeys(list_blob_dot))

#### We now have a list of paths. We will check which one has the preffix
        l_dataset = []
        for blob in list_blob_dot:
            split_ = blob.split("/",  1)
            bucket = split_[0]
            blob_ = split_[1]
            prefix = '{0}/{1}'.format(blob_, dataset_suffix)
            l_blob =  gcp.list_blob(bucket = bucket, prefix = prefix)
            if l_blob['blob']:
                break
        for d in l_blob['blob']:
            split_ = d.rsplit("/",  1)
            l_dataset.append(split_[1])

        template_prefix = """
    df_load_data = pd.DataFrame()
    for dataset in {0}:
    gcp.download_blob(bucket_name = '{1}',
                  destination_blob_name = '{2}',
                  source_file_name = dataset)

    df_temp = pd.read_csv(dataset,
                          compression='gzip',
                          header=0,
                          sep=',',
                          quotechar='"',
                          error_bad_lines=False)
    df_load_data = df_load_data.append(df_temp)
    df_load_data.head()
    """

#### Append to the dic

        ggs_cell = template_prefix.format(l_dataset,bucket, blob_)
        ggs_md = template_gcs_md.format(notebook_name)
        dic_load_data['GCS']['markdown'].append(ggs_md)
        dic_load_data['GCS']['code'].append(ggs_cell)

	### If preffix, then override the final destination for GCP
	### If list dataset has both preffix and not prefix for GCP
	### origin, then preffix overide the destination cell

        dic_destination_inf = {
		'Bucket':bucket,
		'destination_blob_name':blob_,
		'new_destination_blob': blob_.replace('Raw_', 'Processed_')
	}

### Add markdown origin
    template_ds = '\n - {0}'
    l_gcs = []
    gs_url = '[{0}](https://docs.google.com/spreadsheets/d/{1})'
    gbq_url = '[{0}](https://console.cloud.google.com/bigquery?project=valid-pagoda-132423' \
'&p=valid-pagoda-132423&d={1}&t={0}&page=table)'
    coda_url = []
    coda_gs = 'https://docs.google.com/spreadsheets/d/{1}'
    coda_bq = 'https://console.cloud.google.com/bigquery?project=valid-pagoda-132423' \
'&p=valid-pagoda-132423&d={1}&t={0}&page=table'
    for key, value in dic_result.items():
        if key == 'BQ':
            l_gcs.append('\n### Big Query Dataset \n')
            for file in value['Filename']:
                cell_gcs = template_ds.format(file)
                url_bq = gbq_url.format(file,bigquery_dataset[0])
                cell_bq = template_ds.format(url_bq)
                l_gcs.append(cell_bq)
                coda_url.append(coda_bq.format(file,bigquery_dataset[0]))
        elif key == 'GCS':
            l_gcs.append('\n### Google Cloud Storage Dataset \n')
            for file in value['Filename']:
                cell_gcs = template_ds.format(file)
                l_gcs.append(cell_gcs)
            if dataset_suffix:
                cell_gcs = template_ds.format(dataset_suffix)
                l_gcs.append(cell_gcs)
        else:
            l_gcs.append('\n### Google Spreadsheet Dataset \n')
            for i, file in enumerate(value['Filename']):
                cell_gs = template_ds.format(file)
                sheet_name = value['Filename'][i]
                sheet_ID = value['ID'][i]
                url_gs = gs_url.format(sheet_name, sheet_ID)
                cell_gs = template_ds.format(url_gs)
                l_gcs.append(cell_gs)
                coda_url.append(coda_gs.format(sheet_name, sheet_ID))
    cell_datasource = ' '.join(l_gcs)

    template_coda = "Source_data = {}".format(coda_url)

    nb_cells[1][1] = cell_datasource

### Change profiling
    nb_cells[9][1][2] = nb_cells[9][1][2].replace("NAME", notebook_name)

### Upload to the Cloud
    template_upload_bqgcs_md = """

### Move to GCS and BigQuery

We move the dataset to the following:

- **bucket**: *{0}*

- **Destination_blob**: *{1}*
- **name**:  *{2}.gz*
- **Dataset**: *{3}*

- **table**: *{2}*

### GCS

We first need to save *{2}* with `.gz` extension locally then we can move it
to GCS
"""

    template_upload_bqgcs = """
### First save locally
df_final.to_csv(
	'{0}.gz',
	sep=',',
	header=True,
	index=False,
	chunksize=100000,
	compression='gzip',
	encoding='utf-8')

### Then upload to GCS
bucket_name = '{1}'
destination_blob_name = '{2}'
source_file_name = '{0}.gz'
gcp.upload_blob(bucket_name, destination_blob_name, source_file_name)

"""

    template_upload_bq = """

### Move to bigquery
bucket_gcs ='{0}/{1}/{2}.gz'
gcp.move_to_bq_autodetect(dataset_name= '{3}',
							 name_table= '{2}',
							 bucket_gcs=bucket_gcs)
"""

    if destination_engine == 'GCP':
        mkdown_output = template_upload_bqgcs_md.format(dic_destination_inf['Bucket'],
							 dic_destination_inf['new_destination_blob'],
							 notebook_name,
							 bigquery_dataset[0]
							)
        code_output_gcs = template_upload_bqgcs.format(notebook_name,
							 dic_destination_inf['Bucket'],
							 dic_destination_inf['new_destination_blob']
							)
        code_output_bq = template_upload_bq.format(dic_destination_inf['Bucket'],
						  dic_destination_inf['new_destination_blob'],
						  notebook_name,
						  bigquery_dataset[0]
						 )
    else:
        pass

#### Generate nb
    nb = nbf.v4.new_notebook()
    cell_left = 'cell_{0}'
    cell_right = '{0}'


    dic_cells={}
    for i, cell in enumerate(nb_cells[0:6]):
        _source = ''.join(cell[1])
        rhs = cell_right.format(_source)
	#lhs = cell_left.format(i)
        if cell[0] == 'markdown':
            dic_cells["markdown_{}".format(i)] = rhs
        elif cell[0] == 'code':
            dic_cells["code_{}".format(i)] = rhs

### GCS
    next_cells_available = len(dic_load_data['GCS']['markdown']) + 6
    for i, markdown in enumerate(dic_load_data['GCS']['markdown']):
        _md = ''.join(markdown)
        code = dic_load_data['GCS']['code'][i]
        _code = ''.join(code)

        rhs_md = cell_right.format(_md)
        rhs_code = cell_right.format(_code)
	#lhs = cell_left.format(i + 3)
        dic_cells["markdown_{}".format(i+5)] = rhs_md
        dic_cells["code_{}".format(i + 6)] = rhs_code

        next_cells_available = next_cells_available + i

### GBQ
    for i, markdown in enumerate(dic_load_data['BQ']['markdown']):
        _md = ''.join(markdown)
        code = dic_load_data['BQ']['code'][i]
        _code = ''.join(code)

        rhs_md = cell_right.format(_md)
        rhs_code = cell_right.format(_code)
	#lhs = cell_left.format(i + 3)
        dic_cells["markdown_{}".format(i+next_cells_available)] = rhs_md
        dic_cells["code_{}".format(i + next_cells_available + 1)] = rhs_code

        next_cells_available = next_cells_available + 1
    next_cells_available = next_cells_available + 3
### GS
    for i, markdown in enumerate(dic_load_data['GS']['markdown']):
        _md = ''.join(markdown)
        code = dic_load_data['GS']['code'][i]
        _code = ''.join(code)

        rhs_md = cell_right.format(_md)
        rhs_code = cell_right.format(_code)
	#lhs = cell_left.format(i + 3)
        dic_cells["markdown_{}".format(i + next_cells_available)] = rhs_md
        dic_cells["code_{}".format(i + next_cells_available + 1)] = rhs_code

        next_cells_available = next_cells_available + 1
    next_cells_available = next_cells_available + 5

### Add step cells
    _source = ''.join(nb_cells[6][1])
    rhs = cell_right.format(_source)
    dic_cells["markdown_{}".format(next_cells_available)] = rhs
    dic_cells["code_{}".format(next_cells_available)] = []
    next_cells_available = next_cells_available + 2

### profiling

    _source = ''.join(nb_cells[8][1])
    rhs = cell_right.format(_source)
    dic_cells["markdown_{}".format(next_cells_available)] = rhs
    next_cells_available = next_cells_available + 1

    _source = ''.join(nb_cells[9][1])
    rhs = cell_right.format(_source)
    dic_cells["code_{}".format(next_cells_available)] = rhs
    next_cells_available = next_cells_available + 1

### upload to cloud
    _source = ''.join(nb_cells[10][1])
    rhs = cell_right.format(_source)
    dic_cells["markdown_{}".format(next_cells_available)] = rhs
    next_cells_available = next_cells_available + 1

    dic_cells["markdown_{}".format(next_cells_available)] = mkdown_output
    dic_cells["code_{}".format(next_cells_available + 1)] = code_output_gcs
    dic_cells["code_{}".format(next_cells_available + 2)] = code_output_bq

### Data catalogue
    for i, cell in enumerate(nb_cells[-6:]):
        _source = ''.join(cell[1])
        rhs = cell_right.format(_source)
	#lhs = cell_left.format(i)
        if cell[0] == 'markdown':
            dic_cells["markdown_{}".format(next_cells_available+i+ 1)] = rhs
        elif cell[0] == 'code':
            dic_cells["code_{}".format(next_cells_available+i+2)] = rhs

    _code = ''.join(template_coda)
    rhs_code = cell_right.format(_code)
    dic_cells["code_{}".format(next_cells_available+i -1)] = rhs_code


### generate nb
    nb['cells'] = []
    regex = r"^markdown"
    for k, v in dic_cells.items():
        if re.search(regex, k):
            nb['cells'].extend([nbf.v4.new_markdown_cell(v)])
        else:
            nb['cells'].extend([nbf.v4.new_code_cell(v)])
    name_nb = notebook_name + '_preprocessing.ipynb'


    #os.chdir(path_)
    nbf.write(nb, name_nb)

    print("Notebook script {} generated sucessfully.".format(notebook_name))
    project = ''
    template_bq_input ="https://console.cloud.google.com/bigquery?project={0}&p={0}&d={1}&t={2}&page=table"
    bq_input = template_bq_input.format(project, bigquery_dataset, notebook_name)
    template_gsutil = "gs//{0}/{1}/{2}.gz"
    if dataset_suffix:
        template_gsutil = template_gsutil.format(dic_destination_inf['Bucket'],
        dic_destination_inf['destination_blob_name'], dataset_suffix)
    else:
        template_gsutil = template_gsutil.format(dic_destination_inf['Bucket'],
        dic_destination_inf['destination_blob_name'], 'UNKNOWN')

    studio_ ="auto-nb-studio -u '{0}' -c 'GCP' -n '{1}' -p '{2}' -d 'ADD DATE VAR' -m 'US' -t '/PATH CREDENTIAL'"
    studio_ = studio_.format(username, notebook_name, bigquery_dataset)

    print( """
Please, go to the section Inputs_datasource from
    https://coda.io/d/MasterFile-Database_dvfMWDBnHh8/Main_suj76#_lubx_ \n
 paste the following information \n
 Storage:
 - Storage: GCS
 - Top_level: {0}
 - Path: {1}
 - Filename: {2}.gz
 - Description: Add detailed description
 - Dataset_documentation: If any, add URL
 - Size: Go to console to get the size
 - Status: Closed
 - Source_data:{3}
 - Link_methodology: If any, add URL
 - JupyterStudio: Leave unselect
 - Profiling: Leave unselect \n
 Big Query :
  - Storage: BQ
  - Top_level: {5}
  - Path: None
  - Filename: {2}
  - Source_data:{4}
  - JupyterStudio: Use \n
 {6} \n
  to create a notebook for the studio, then select the checkbox. Only if Dataset in BigQuery!
 - Profiling: select the checkbox
      """.format(dic_destination_inf['Bucket'], dic_destination_inf['new_destination_blob'],
     notebook_name, template_gsutil, bq_input, bigquery_dataset, studio_)
     )
