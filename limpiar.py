import shutil
from config import DATA

shutil.rmtree(DATA, ignore_errors=True)
print("data/ eliminada. Todo listo para empezar de nuevo.")
