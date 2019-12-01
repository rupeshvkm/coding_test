import smartsheet
import boto3


smart = smartsheet.Smartsheet()

smart.errors_as_exceptions(True)
_dir = ''
result = smart.Sheets.import_xlsx_sheet(_dir+'/sample_sheet.xlsx',header_row_index=0)
sheet  = smart.sheets.get_sheet(result.data.id)

bucket = 'test_bucket'
key = 'test_key'

s3 = boto3.resource('s3')
object = s3.Object(bucket,key)