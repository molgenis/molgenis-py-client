
def cleanUrl(url: str=None):
    """Clean URL
    Remove trailing slash if present
    
    @param url string containing an URL
    
    @return string
    """
    return url[0:len(url)-1] if url[-1:]=='/' else url


class toCsvString:
    
    def __init__(self, data: list=[]):
        """To CSV String
        Convert a dataset to a comma-separated string
        
        @param data dataset to convert (i.e., list of dictionaries)
        
        @return string; comma-separated string
        """
        self.data = data
        self.keyset = None
        self.csv = None
        
    def createKeySet(self):
        """Create Key Set
        Collate a list of all keys in a dataset
        """
        keys = []
        for row in self.data:
          for key in row.keys():
              if key not in keys:
                  keys.append(key)
        self.keyset = keys

    def applyMissingKeys(self):
        """Apply Missing Keys
        Add missing keys where applicable. This ensures each dictionary has the
        same shape.
        """
        data = self.data
        for row in data:
           for key in self.keyset:
               if key not in row.keys():
                   row[key] = None
        self.data = data
    
    def write(self):
        """Write
        Write dataset as csv string
        """
        self.createKeySet()
        self.applyMissingKeys()

        rows=[]
        for row in self.data:
          rowAsString=[]
          for key in self.keyset:
              rowAsString.append(row[key])
          rows.append(','.join(map(str, rowAsString)))
        
        header= ','.join(self.keyset)
        body ='\n'.join(rows)
        self.csv = f"{header}\n{body}\n"
