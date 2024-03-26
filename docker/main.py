import pandas as pd
# import pytokenjoin as ptj
from minio import Minio
import json
import uuid
import sys
import importlib
import traceback
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import precision_score, recall_score, f1_score

def prep_df(input_file, header, col_text, col_separator, text_separator, minio):
    """
    Prepare DataFrame from input file.
    """
    
    header = header if header != -1 else None
    
    if input_file.startswith('s3://'):
        bucket, key = input_file.replace('s3://', '').split('/', 1)
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        df = pd.read_csv(client.get_object(bucket, key), header=header, sep=col_separator, on_bad_lines = 'warn')
    else:
        df = pd.read_csv(input_file, header=header, sep=col_separator, on_bad_lines = 'warn')
    df.columns = [str(c) for c in df.columns]
    
    df[f"{col_text}_original"] = df[col_text].copy()
    df[col_text] = df[col_text].str.split(text_separator)
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
            col_id_left = str(j['parameters']['col_id_left'])
            col_text_left = str(j['parameters']['col_text_left'])
            col_separator_left = j['parameters']['col_separator_left']        
            col_ground_left = str(j['parameters'].get('col_ground_left', None))
            text_separator_left = j['parameters']['text_separator_left']        
            header_left = j['parameters']['header_left']
            input_file_right = inputs[1]
            col_id_right = str(j['parameters']['col_id_right'])
            col_text_right = str(j['parameters']['col_text_right'])
            col_separator_right = j['parameters']['col_separator_right']                
            text_separator_right = j['parameters']['text_separator_right']            
            header_right = j['parameters']['header_right']
            output_file = j['parameters']['output_file']
            
            df_left = prep_df(input_file_left, header_left, col_text_left, col_separator_left, text_separator_left, minio)
            df_right = prep_df(input_file_right, header_right, col_text_right, col_separator_right, text_separator_right, minio)
            
            left_attr = [f"{col_text_left}_original"]
            if col_ground_left is not None:
                left_attr.append(col_ground_left)
            right_attr = [f"{col_text_right}_original"]
            
            if method == 'delta':
                delta = j['parameters']['delta']     
                posFilter = j['parameters']['posFilter']     
                jointFilter = j['parameters']['jointFilter']   
                pairs, log = module.tokenjoin_foreign(df_left, df_right, 
                                                      col_id_left, col_id_right,
                                                      col_text_left, col_text_right,
                                                      delta=delta, posFilter = posFilter,
                                                      jointFilter = jointFilter,
                                                      keepLog=True,
                                                      left_attr=left_attr, right_attr=right_attr)
            elif method == 'topk':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                pairs, log = module.tokenjoin_foreign(df_left, df_right, 
                                                      col_id_left, col_id_right,
                                                      col_text_left, col_text_right,
                                                      k=k, delta_alg=delta_alg, keepLog=True,
                                                      left_attr=left_attr, right_attr=right_attr)
                
            elif method == 'knn':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                
                pairs, log = module.tokenjoin_foreign(df_left, df_right, 
                                                      col_id_left, col_id_right,
                                                      col_text_left, col_text_right,
                                                      k=k, delta_alg=delta_alg, keepLog=True,
                                                      left_attr=left_attr, right_attr=right_attr)
                
            if col_ground_left is not None:
                mlb = MultiLabelBinarizer()
                y_true_bin = mlb.fit_transform(pairs[f"l_{col_ground_left}"])
                y_pred_bin = mlb.transform(pairs[f"r_{col_text_right}_original"])

                log = {'total_time': log['total_time']}
                for avg in ['micro', 'macro', 'weighted']:
                    log[f'precision_{avg}'] = precision_score(y_true_bin, y_pred_bin, average=avg)
                    log[f'recall_{avg}'] = recall_score(y_true_bin, y_pred_bin, average=avg)
                    log[f'f1_{avg}'] = f1_score(y_true_bin, y_pred_bin, average=avg)
                
        else:
            input_file = inputs[0]
            col_id = str(j['parameters']['col_id'])
            col_text = str(j['parameters']['col_text'])
            col_separator = j['parameters']['col_separator']
            col_ground = str(j['parameters'].get('col_ground', None))
            text_separator = j['parameters']['text_separator']            
            output_file = j['parameters']['output_file']
            
            df = prep_df(input_file, col_text, col_separator, text_separator, minio)
            attr = [f"{col_text}_original"]
            if col_ground is not None:
                attr.append(col_ground)
            
            if method == 'delta':
                delta = j['parameters']['delta']     
                posFilter = j['parameters']['posFilter']     
                jointFilter = j['parameters']['jointFilter']     
                pairs, log = module.tokenjoin_self(df, col_id, col_text,
                                                   delta=delta, posFilter = posFilter,
                                                   jointFilter = jointFilter,
                                                   keepLog=True, attr=attr)
            elif method == 'topk':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                pairs, log = module.tokenjoin_self(df, col_id, col_text,
                                                   k=k, delta_alg=delta_alg, 
                                                   keepLog=True, attr=attr)
                
            elif method == 'knn':
                k = j['parameters']['k']
                delta_alg = j['parameters']['delta_alg']
                
                pairs, log = module.tokenjoin_self(df, col_id, col_text,
                                                   k=k, delta_alg=delta_alg,
                                                   keepLog=True, attr=attr)    
            if col_ground is not None:
                mlb = MultiLabelBinarizer()
                y_true_bin = mlb.fit_transform(pairs[f"l_{col_ground}"])
                y_pred_bin = mlb.transform(pairs[f"r_{col_text}_original"])

                log = {'total_time': log['total_time']}
                for avg in ['micro', 'macro', 'weighted']:
                    log[f'precision_{avg}'] = precision_score(y_true_bin, y_pred_bin, average=avg)
                    log[f'recall_{avg}'] = recall_score(y_true_bin, y_pred_bin, average=avg)
                    log[f'f1_{avg}'] = f1_score(y_true_bin, y_pred_bin, average=avg)

        pairs.to_csv(output_file)

        # basename = output_file.split('/')[-1]
        basename = str(uuid.uuid4()) + "." + output_file.split('.')[-1]
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        result = client.fput_object(minio['bucket'], basename, output_file)
        object_path = f"s3://{result.bucket_name}/{result.object_name}"

        return {'message': 'TokenJoin project executed successfully!',
                'output': [{'name': 'List of joined entities', 'path': object_path}], 'metrics': log, 'status': 200}
    except Exception as e:
        print(traceback.format_exc())
        return {
            'message': 'An error occurred during data processing.',
            'error': traceback.format_exc(),
            'status': 500
        }
    
if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise ValueError("Please provide 2 files.")
    with open(sys.argv[1]) as o:
        j = json.load(o)
    response = run(j)
    with open(sys.argv[2], 'w') as o:
        o.write(json.dumps(response, indent=4))
