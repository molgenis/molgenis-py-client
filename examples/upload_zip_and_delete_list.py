import molgenis.client as molgenis

session = molgenis.Session("http://localhost:8080/")
session.login('admin', 'admin')

response = session.upload_zip("resources/upload.zip").split('/')
runEntityType = response[-2]
runId = response[-1]
statusInfo = session.get_by_id(runEntityType, runId)
count = 1

print("Uploading org_molgenis_test_python_TypeTest")
while statusInfo['status'] == 'RUNNING':
    count += 1
    print('.')
    statusInfo = session.get_by_id(runEntityType, runId)
    if statusInfo["status"] == "FINISHED":
        print("org_molgenis_test_python_TypeTest uploaded")
    if statusInfo["status"] == "FAILED":
        print("Failed: ", statusInfo['message'])

session.delete_list("org_molgenis_test_python_TypeTest", ["1", "2", "3"])
print("deleted first three rows")
