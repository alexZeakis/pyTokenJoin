import pandas as pd
# import pytokenjoin as ptj
from minio import Minio
import json
import uuid
import sys
import importlib

def prep_df(input_file, col_text, separator, minio):
    """
    Prepare DataFrame from input file.
    """
    if input_file.startswith('s3://'):
        bucket, key = input_file.replace('s3://', '').split('/', 1)
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        df = pd.read_csv(client.get_object(bucket, key), header=None)
    else:
        df = pd.read_csv(input_file, header=None)
    
    col_text = df.columns[col_text]
    df[col_text] = df[col_text].str.split(separator)
    df = df.loc[~(df[col_text].isna())]
    df[col_text] = df[col_text].apply(lambda x: list(set(x)))
    
    df.columns = [str(col) for col in df.columns]
    return df

def run(j):
    try:
        inputs = j['input']
        
        foreign = j['parameters']['foreign']
        if foreign not in ['self', 'foreign']:
            raise ValueError("Foreign must be in ['self', 'foreign']")
        similarity = j['parameters']['similarity']
        if similarity not in ['jaccard', 'edit']:
            raise ValueError("Similarity must be in ['jaccard', 'edit']")        
        method = j['parameters']['method']
        if method not in ['delta', 'topk', 'knn']:
            raise ValueError("Method must be in ['delta', 'topk', 'knn']")
        
        minio = j['minio']
        
        module = importlib.import_module('pytokenjoin.' + similarity + '.join_' + method)
        module = module.TokenJoin()
        
        if foreign == 'foreign':
            input_file_left = inputs[0]
            col_id_left = j['parameters']['col_id_left']
            col_text_left = j['parameters']['col_text_left']
            separator_left = j['parameters']['separator_left']        
            input_file_right = inputs[1]
            col_id_right = j['parameters']['col_id_right']
            col_text_right = j['parameters']['col_text_right']        
            separator_right = j['parameters']['separator_right']                
            output_file = j['parameters']['output_file']
            
            df_left = prep_df(input_file_left, col_text_left, separator_left, minio)
            df_right = prep_df(input_file_right, col_text_right, separator_right, minio)
            
            if method == 'delta':
                delta = j['parameters']['delta']     
                posFilter = j['parameters']['posFilter']     
                jointFilter = j['parameters']['jointFilter']   
                pairs, log = module.tokenjoin_foreign(df_left, df_right, 
                                                      str(col_id_left), str(col_id_right),
                                                      str(col_text_left), str(col_text_right),
                                                      delta=delta, posFilter = posFilter,
                                                      jointFilter = jointFilter,
                                                      keepLog=True)
            elif method == 'topk':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                pairs, log = module.tokenjoin_foreign(df_left, df_right, 
                                                      str(col_id_left), str(col_id_right),
                                                      str(col_text_left), str(col_text_right),
                                                      k=k, delta_alg=delta_alg, keepLog=True)
                
            elif method == 'knn':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                
                pairs, log = module.tokenjoin_foreign(df_left, df_right, 
                                                      str(col_id_left), str(col_id_right),
                                                      str(col_text_left), str(col_text_right),
                                                      k=k, delta_alg=delta_alg, keepLog=True)
        else:
            input_file = inputs[0]
            col_id = j['parameters']['col_id']
            col_text = j['parameters']['col_text']
            separator = j['parameters']['separator']
            output_file = j['parameters']['output_file']
            
            df = prep_df(input_file, col_text, separator, minio)
            
            if method == 'delta':
                delta = j['parameters']['delta']     
                posFilter = j['parameters']['posFilter']     
                jointFilter = j['parameters']['jointFilter']     
                pairs, log = module.tokenjoin_self(df, str(col_id), str(col_text),
                                                   delta=delta, posFilter = posFilter,
                                                   jointFilter = jointFilter,
                                                   keepLog=True)
            elif method == 'topk':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                pairs, log = module.tokenjoin_self(df, str(col_id), str(col_text),
                                                   k=k, delta_alg=delta_alg, keepLog=True)
                
            elif method == 'knn':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                
                pairs, log = module.tokenjoin_self(df, str(col_id), str(col_text),
                                                   k=k, delta_alg=delta_alg, keepLog=True)                

        pairs.to_csv(output_file)

        # basename = output_file.split('/')[-1]
        basename = str(uuid.uuid4()) + "." + output_file.split('.')[-1]
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        result = client.fput_object(minio['bucket'], basename, output_file)
        object_path = f"s3://{result.bucket_name}/{result.object_name}"

        return {'message': 'TokenJoin project executed successfully!',
                'output': [{'name': 'List of joined entities', 'path': object_path}], 'metrics': log, 'status': 200}
    except Exception as e:
        return {
            'message': 'An error occurred during data processing.',
            'error': str(e),
            'status': 500
        }
    
if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise ValueError("Please provide 2 files.")
    with open('./logs/'+sys.argv[1]) as o:
        j = json.load(o)
    response = run(j)
    with open('./logs/'+sys.argv[2], 'w') as o:
        o.write(json.dumps(response, indent=4))
