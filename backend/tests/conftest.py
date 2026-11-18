import os,sys,tempfile,shutil
ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,ROOT)
TMP=tempfile.mkdtemp(prefix='archive-test-')
os.environ.setdefault('JWT_SECRET','archive-test-secret')
os.environ['DATABASE_URL']='sqlite:///'+TMP+'/test.db'
os.environ['ARCHIVE_DIR']=TMP+'/archive'
os.environ['ARCHIVE_CAPACITY_BYTES']=str(1024*1024)
os.environ['ARCHIVE_RETENTION_DAYS']='30'
os.environ['ARCHIVE_PIN_TTL_SECONDS']='300'
import pytest
from src.config.database import Base,engine,Session
from src.models import identity,batch,mapping
from src.models import run as run_model
from src.models import archive as archive_model
from src.services import archive
MODELS=(archive_model.ArchivePin,archive_model.ArchiveRef,archive_model.ArchiveObject,run_model.TargetRow,run_model.Run,mapping.Mapping,batch.Batch,identity.Audit,identity.User)
@pytest.fixture(autouse=True)
def clean():
 Base.metadata.create_all(engine)
 with Session() as s:
  for t in MODELS:s.query(t).delete()
  s.commit()
 shutil.rmtree(os.environ['ARCHIVE_DIR'],ignore_errors=True)
 yield
@pytest.fixture()
def mkrun():
 def make(result=None):
  with Session() as s:
   r=run_model.Run(name='run-'+os.urandom(4).hex(),batch_id=1,mapping_id=1,status='completed',snapshot={},result=result)
   s.add(r);s.commit();s.refresh(r);return r.id
 return make
