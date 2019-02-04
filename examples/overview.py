import pprint

import molgenis.client as molgenis

# Save variables used through the entire script:
arguments = {"entityType": "demo_sightings",
             "filename": "sightings",
             "id": "1",
             "username": "admin",
             "password": "admin",
             "url": "http://localhost:8080/api/",
             "updateRow": {"id": "to be filled in", "year": "1998", "type": "vampire"},
             "updateRows": [{"id": "to be filled in", "year": "1998", "type": "gnome"},
                            {"id": "to be filled in", "year": "1998", "type": "fairy"},
                            {"id": "to be filled in", "year": "1998", "type": "unicorn"}],
             "updateAttr": "year",
             "updateVal": "2000"
             }

print("Running demonstration of MOLGENIS python API with following arguments: ")
for arg in arguments:
    print('{}:{}'.format(arg, arguments[arg]))

# To run this script you should have molgenis running locally, otherwise replace the url below with the url of your
# server
session = molgenis.Session(arguments["url"])

# Login
session.login(arguments["username"], arguments["password"])
print("\nYou are logged in as: {}".format(arguments["username"]))

# Upload zip
response = session.upload_zip("resources/{}.zip".format(arguments["filename"])).split("/")
# An upload is asynchronous, so you should check when it is done if you want to work with the data further on.
# The response of this query is the location of the row in the importRun entity via the REST api
runEntityType = response[-2]
runId = response[-1]
statusInfo = session.get_by_id(runEntityType, runId)
count = 1
print("\r{} uploading{}".format(arguments["entityType"], count * "."), end='')
while statusInfo['status'] == 'RUNNING':
    count += 1
    print("\r{} uploading{}".format(arguments["entityType"], count * "."), end='')
    statusInfo = session.get_by_id(runEntityType, runId)
    if statusInfo["status"] == "FINISHED":
        print("Done!")
    if statusInfo["status"] == "FAILED":
        print("Failed: ", statusInfo['message'])

# Get the a row by id, returns the selected row as dictionary
row = session.get_by_id(arguments["entityType"], arguments["id"])
print("\nRow: {} of entityType: {}".format(arguments["id"], arguments["entityType"]))
pprint.pprint(row)

# Get the table you just uploaded. It will be returned in a list of all entities represented as dictionaries.
table = session.get(arguments["entityType"], num=1000)
print("\nEntityType: {}".format(arguments["entityType"]))
pprint.pprint(table)

# Add a row to the table you uploaded.
arguments["updateRow"]["id"] = str(len(table) + 1)
session.add(arguments["entityType"], arguments["updateRow"])
print("\nEntityType: {} updated with: {}".format(arguments["entityType"], arguments["updateRow"]))

# Update one value of a row
session.update_one(arguments["entityType"], arguments["updateRow"]["id"], arguments["updateAttr"],
                   arguments["updateVal"])
print("\nEntityType: {} attribute {} is altered from {} to {}".format(arguments["entityType"], arguments["updateAttr"],
                                                                      arguments["updateRow"][arguments["updateAttr"]],
                                                                      arguments["updateVal"]))

# Add several rows to the table you uploaded.
arguments["updateRows"][0]["id"] = str(len(table) + 2)
arguments["updateRows"][1]["id"] = str(len(table) + 3)
arguments["updateRows"][2]["id"] = str(len(table) + 4)
print("\nEntityType: {} updated with:".format(arguments["entityType"]))
pprint.pprint(arguments["updateRows"])

# Delete list of entities
ids = [value['id'] for value in arguments["updateRows"]]
session.delete_list(arguments["entityType"], ids)
print("\nDeleted rows with ids from entityType: {}: {} (the ones that were just added using add_all)".format(
    arguments["entityType"], ids))

# Delete one row based on id
rowToDelete = table[0]["id"]
session.delete(arguments["entityType"], rowToDelete)
print("\nDeleted row with id: {} from entityType: {}".format(rowToDelete, arguments["entityType"]))

# Get metadata of entity
entity_meta = session.get_entity_meta_data(arguments["entityType"])
print("\nRetrieved metadata for entityType: {}".format(arguments["entityType"]))
pprint.pprint(entity_meta)

# Get metadata of attribute in entity
attribute_meta = session.get_attribute_meta_data(arguments["entityType"], arguments["updateAttr"])
print("\nRetrieved metadata for attribute: {} of entityType: {}".format(arguments["updateAttr"],
                                                                        arguments["entityType"]))
pprint.pprint(attribute_meta)
