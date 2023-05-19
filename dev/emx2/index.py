
from molgenis.emx2.client import Molgenis

# conect to an emx2 instance
db = Molgenis(host='....')
db.signin(username='...',password='...')


# create new tags and import
newTags = [
    {'name': 'brown', 'parent': 'colors'},
    {'name': 'canis', 'parent': 'species'},
]

db.add(schema='pet store', table='Tag', data = newTags)
            

# create new pets with tags and import
newPets = [
    {
        'name': 'Snuffy',
        'category': 'dog',
        'status': 'available',
        'weight': 6.8,
        'tags': 'brown,canis'
    }
]

db.add(schema='pet%20store', table='Pet', data=newPets)


# retrieve data
data = db.get(schema="pet%20store", table="Pet")
