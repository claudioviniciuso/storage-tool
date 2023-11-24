import pandas as pd
import json
import io

class DataProcessor:
    def process_data(self, data_bytes, file_extension, return_type=None):
        if file_extension == 'json':
            return self._process_json(data_bytes, return_type)
        elif file_extension == 'csv':
            return self._process_csv(data_bytes, return_type)
        elif file_extension == 'xlsx':
            return self._process_excel(data_bytes, return_type)
        elif file_extension == 'parquet':
            return self._process_parquet(data_bytes, return_type)
        elif file_extension == 'txt':
            return self._process_txt(data_bytes, return_type)
        else:
            raise ValueError('file_extension must be json, csv, xlsx, parquet or txt')

    def _process_json(self, data_bytes, return_type=dict):
        data = json.loads(data_bytes)
        if return_type == dict:
            return data
        elif return_type == pd.DataFrame:
            return pd.DataFrame(data)
        else:
            raise ValueError('return_type must be dict or pd.DataFrame')

    def _process_csv(self, data_bytes, return_type=pd.DataFrame):
        data = pd.read_csv(io.BytesIO(data_bytes))
        if return_type == pd.DataFrame:
            return data
        elif return_type == dict:
            return data.to_dict()
        else:
            raise ValueError('return_type must be dict or pd.DataFrame')

    def _process_excel(self, data_bytes, return_type=pd.DataFrame):
        data = pd.read_excel(io.BytesIO(data_bytes))
        if return_type == pd.DataFrame:
            return data
        elif return_type == dict:
            return data.to_dict()
        else:
            raise ValueError('return_type must be dict or pd.DataFrame')
        
    def _process_parquet(self, data_bytes, return_type=pd.DataFrame):
        data = pd.read_parquet(io.BytesIO(data_bytes))
        if return_type == pd.DataFrame:
            return data
        elif return_type == dict:
            return data.to_dict()
        else:
            raise ValueError('return_type must be dict or pd.DataFrame')
    
    def _process_txt(self, data_bytes, return_type=pd.DataFrame):
        data = pd.read_csv(io.BytesIO(data_bytes), sep='\t')
        if return_type == pd.DataFrame:
            return data
        elif return_type == dict:
            return data.to_dict()
        else:
            raise ValueError('return_type must be dict or pd.DataFrame')
    
    def convert_to_bytes(self, data, file_extension):

        if isinstance(data, pd.DataFrame):
            if file_extension == 'csv':
                return data.to_csv(index=False).encode('utf-8')
            elif file_extension == 'xlsx':
                return data.to_excel(index=False)
            elif file_extension == 'parquet':
                return data.to_parquet(index=False)
            elif file_extension == 'json':
                return data.to_json(index=False).encode('utf-8')
            elif file_extension == 'txt':
                return data.to_csv(index=False, sep='\t').encode('utf-8')
            else:
                raise ValueError('file_extension must be json, csv, xlsx, parquet or txt')
        
        elif isinstance(data , dict) or isinstance(data, list):
            if file_extension == 'json':
                return json.dumps(data).encode('utf-8')
            elif file_extension == 'txt':
                return pd.DataFrame(data).to_csv(index=False, sep='\t').encode('utf-8')
            elif file_extension == 'csv':
                return pd.DataFrame(data).to_csv(index=False).encode('utf-8')
            elif file_extension == 'xlsx':
                return pd.DataFrame(data).to_excel(index=False)
            elif file_extension == 'parquet':
                return pd.DataFrame(data).to_parquet(index=False)
            else:
                raise ValueError('file_extension must be json, csv, xlsx, parquet or txt')
            
        
        else:
            raise ValueError('data must be dict or pd.DataFrame')
        
        


