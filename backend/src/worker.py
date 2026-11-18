import time,logging,redis
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.services.migration import migrate,make_backup,key_digest,Cancelled
from src.services import archive
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
def progress(run_id,info):
 Path('/tmp/worker-heartbeat').touch()
 with Session() as s:
  r=s.get(Run,run_id)
  if r.cancel_requested or r.status=='cancelled':raise Cancelled()
  r.progress=info;s.commit()
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:
  item=q.blpop('migration-runs',timeout=2)
  if not item:continue
  run_id=int(item[1])
  with Session() as s:
   r=s.query(Run).filter_by(id=run_id).with_for_update().first()
   if not r or r.status!='queued':continue
   r.status='running';snapshot=r.snapshot;s.commit()
  try:
   result=migrate(snapshot['batch']['rows'],snapshot['mapping']['rules'],snapshot['mapping']['chunk_size'],lambda info:progress(run_id,info))
   body=make_backup(result,run_id,snapshot['batch']['name'])
   try:
    info=archive.archive_backup(run_id,body,snapshot.get('retention_days'))
    result['backup_archive']=info
    log('worker','run.archived',f"{run_id}:{info['digest'][:12]}")
    if info.get('reclaimed'):log('worker','archive.reclaimed',f"{run_id} 容量回收 {len(info['reclaimed'])} 份过期正文")
   except archive.CapacityRejected as e:
    result['backup']=body;result['backup_archive']={'status':'rejected','reason':str(e)}
    log('worker','archive.rejected',f"{run_id}:{e}")
   except Exception as e:
    logging.exception('archive failed')
    result['backup']=body;result['backup_archive']={'status':'error','reason':str(e)[:300]}
   with Session() as s:
    r=s.query(Run).filter_by(id=run_id).with_for_update().first()
    if r.cancel_requested:raise Cancelled()
    for i,row in enumerate(result['accepted_rows']):s.add(TargetRow(run_id=run_id,ordinal=i,key_digest=key_digest(row),payload=row))
    r.result=result;r.status='completed';s.commit()
   log('worker','run.completed',str(run_id))
  except Cancelled:
   with Session() as s:r=s.get(Run,run_id);r.status='cancelled';r.error='用户取消，未提交目标行';s.commit()
   log('worker','run.cancelled',str(run_id))
  except Exception as exc:
   logging.exception('migration failed')
   with Session() as s:r=s.get(Run,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
 except Exception:logging.exception('worker loop');time.sleep(2)
