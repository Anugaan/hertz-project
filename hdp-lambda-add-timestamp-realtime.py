import json
import base64
from datetime import datetime

def lambda_handler(event,context):
    try:

        print("*************** Captured Event from Kineis Data Stream ****************")
        print(event)

        data_arr = []

        """
        Loop on number of records in event
        """
        for record in event['records']:
            print("in the record loop")
            event_record_id = record['recordId']
            
            payload = base64.b64decode(record['data']).decode('utf-8')
            payload = payload.replace("\'", "\"")
            
            data = json.loads(payload)
            
            
            parentAttr = list(data.keys())[0]
            print("********* parent attribute **************")
            print(parentAttr)
            if isinstance(data.get(parentAttr),dict) and data.get(parentAttr).get("ApproximateArrivalTimestamp") != None :
                print("************ Reading Mulesoft timestamp *****************")
                data["arrival_timestamp"] = data.get(parentAttr).get("ApproximateArrivalTimestamp")
            else :
                print("************ Reading Kinesis timestamp *****************")
                time_to_fetch = record['approximateArrivalTimestamp']
                leading = str(record['approximateArrivalTimestamp'])[0: 10]

                # get last 3 digits
                trailing = str(record['approximateArrivalTimestamp'])[-3:]
                time = datetime.fromtimestamp(float(leading)).strftime('%Y-%m-%d %H:%M:%S.') + ('%s' % int(trailing))
            
                data["arrival_timestamp"] = time
                
            print("****** record modified **********")
            
            data_arr.append(data)
            
        print("************ data array ******************")
        print(data_arr)
        
        output_record = [{
                'recordId': event['records'][0]['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(json.dumps(data_arr).encode('utf-8')).decode('utf-8')
            }]
            
        event['records'] = output_record
        
        
        print(event)

        return event

    except Exception as ex:
        print("Exception occurred while inserting arrival timestamp")
        print(ex)

        """
        If exception occurred, we are not blocking the data but sending the data as it is
        """

        return {'records': event['records']}
