import tarfile
import zipfile

def decompress(fn):
    """
    Given a tar.gz or a zip, extract the contents and return a list of files

    """
    if zipfile.is_zipfile(fn):
        pass
    elif tarfile.is_tarfile(fn):
        pass
    else:
        raise ValueError('Invalid file type - must be tar.gz or zip')
    

def rast2csv():
    pass
