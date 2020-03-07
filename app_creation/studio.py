import ipywidgets as widgets
import pandas as pd
from ipywidgets import Button, Layout
from IPython.display import display
import re, json, datetime, argparse, sys, os, ast, shutil
import nbformat as nbf
from Fast_connectCloud import connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_platform import connect_cloud_platform

class connector_notebook:
    def __init__(self):
        """
        """
        self.params = params


    def generate_notebook_studio(self):
          """
          """

          #notebook_name = self.params['notebook_name']
          project_name = self.params['project_name']
          input_datasets = self.params['input_datasets'][0] ### Only one dataset, but its a list
          sheetnames = self.params['sheetnames'][0]### Only one sheet, but its a list
          bigquery_dataset = self.params['bigquery_dataset']
          destination_engine = self.params['destination_engine']
          path_notebook = self.params['path_notebook_studio']
          project= 'valid-pagoda-132423'
          username = "thomas"
          pathtoken = self.params['pathtoken']
          connector_en = self.params['connector']
          labels = self.params['labels']
          date_var = self.params['date_var']

          with open(path_notebook) as f:
              ipynb = json.load(f)

          nb_cells = []
          output_str = ""

      # Parse cells in Jupyter notebook template
          for cell in ipynb['cells']:
              cell_type = cell['cell_type']
              source = cell['source']
              nb_cells.append([cell_type, source])
          name_project = project_name.replace(' ', '_')
      ## 1 Change the header
          nb_cells[0][1][0] = nb_cells[0][1][0].replace('TITLE', name_project)
          now = datetime.datetime.now()
          nb_cells[0][1][2] = nb_cells[0][1][2].replace('XX', now.strftime("%Y-%m-%d %H:%M %Z"))

      ## 2 change connector
          if connector_en == 'GS':
              nb_cells[5][1][1] = nb_cells[5][1][1].replace("()", "('GS')")
          else:
              nb_cells[5][1][1] = nb_cells[5][1][1].replace("()", "('GCP')")

      ## Connect to Google: Token is indicated by the user in the command line
          gs = connector.open_connection(online_connection = False,
              path_credential = pathtoken)
          if connector_en == 'GS':
              service_gd = gs.connect_remote(engine = 'GS')
              gdr = connect_drive.connect_drive(service_gd['GoogleDrive'])
          else:
              service_gcp = gs.connect_remote(engine = 'GCP')
              gcp = connect_cloud_platform.connect_console(project = project,
                                                   service_account = service_gcp['GoogleCloudP'])

      ### Change cell data source, var name and load data

          if connector_en == 'GS':


              sID = gdr.find_file_id(file_name=input_datasets, to_print=True)
              url_spreadsheet = 'https://docs.google.com/spreadsheets/d/' + sID



              print('Loading data, be patient')
          ### get n rows and columns
              try:
                  nb_cols, n_row = gdr.getRowAndColumns(sheetID = sID,
                           sheetName = sheetnames)
              except:
                  print('Method 1 failed, trying Method 2')
                  n_row = gdr.getLatestRow(sID, sheetnames)
                  nb_cols = gdr.getColumnNumber(sID, sheetnames)

              load_data = gdr.upload_data_from_spreadsheet(sheetID = sID,
               sheetName = sheetnames,
               to_dataframe = True)

              load_data = load_data.apply(pd.to_numeric, errors='ignore')

              cell_upload_template = """
      from GoogleDrivePy.google_drive import connect_drive
      gdr = connect_drive.connect_drive(service['GoogleDrive'])

      df_final = gdr.upload_data_from_spreadsheet(sheetID = '{0}',
           sheetName = '{1}',
           to_dataframe = True)

      df_final = df_final.apply(pd.to_numeric, errors='ignore')
      df_final.head()
          """
              cell_upload = cell_upload_template.format(sID, sheetnames)

          # Change connector cell
              nb_cells[6][1][4] = nb_cells[6][1][4].replace('XXX', input_datasets)
              nb_cells[6][1][4] = nb_cells[6][1][4].replace('HERE', url_spreadsheet)

              l_dtypes = load_data.dtypes.to_list()

          else:

          #### Need to find the dataset from the table

              dtasets = gcp.list_dataset()
              for d in dtasets['Dataset']:
                  all_tables = gcp.list_tables(dataset = d)['tables']
                  if input_datasets in all_tables:
                      dataset = d

              query = (
                "SELECT * "
                  "FROM {0}.{1} "

              )

              query = query.format(dataset, input_datasets)

              print('Loading data, be patient')
              load_data = gcp.upload_data_from_bigquery(query = query,
               location = 'US')

              l_dtypes = load_data.dtypes.to_list()

              cell_upload_template = """
      from GoogleDrivePy.google_platform import connect_cloud_platform
      project = '{2}'
      gcp = connect_cloud_platform.connect_console(project = project,
                                                   service_account = service['GoogleCloudP'])
      query = (
                "SELECT * "
                  "FROM {0}.{1} "

              )

      df_final = gcp.upload_data_from_bigquery(query = query, location = 'US')
      df_final.head()
          """
              cell_upload = cell_upload_template.format(dataset, input_datasets, project[0])
              nb_cells[6][1][4] = nb_cells[6][1][4].replace('XXX', input_datasets)

          nb_cells[9][1] = cell_upload

      ### Set if label exists
          if labels:

              if len(labels) != len(l_dtypes):
                  print('ERROR: Data source has {0} variables and you input {1} labels'.format(
                  len(l_dtypes), len(labels)
          ))
                  sys.exit()

          else:
              labels = list(load_data)


          dic_name = {'Variables':  list(load_data),
                 'Labels' : labels,
                 'Types': l_dtypes}
          df_ = pd.DataFrame(dic_name, index = None)
          df_html = df_.to_html()
          nb_cells[7][1] = df_html

          ## Change Year
          nb_cells[14][1][4] = nb_cells[14][1][4].replace('CHANGE VARIABLE', date_var)

          ## generate notebook
          nb = nbf.v4.new_notebook()
          cell_left = 'cell_{0}'
          cell_right = '{0}'

          dic_cells={}
          for i, cell in enumerate(nb_cells):
              _source = ''.join(cell[1])
              rhs = cell_right.format(_source)
              lhs = cell_left.format(i)
              if cell[0] == 'markdown':
                  dic_cells["markdown_{}".format(i)] = rhs
              elif cell[0] == 'code':
                  dic_cells["code_{}".format(i)] = rhs

          nb = nbf.v4.new_notebook()
          nb['cells'] = []
          regex = r"^markdown"
          for k, v in dic_cells.items():
              if re.search(regex, k):
                  nb['cells'].extend([nbf.v4.new_markdown_cell(v)])
              else:
                  nb['cells'].extend([nbf.v4.new_code_cell(v)])
          name_nb = name_project + '_studio.ipynb'
          nbf.write(nb, name_nb)
