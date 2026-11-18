import json
import redis
from datetime import datetime,timezone
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL,ARCHIVE_RETENTION_DAYS
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.utils.serialization import record
from src.services import archive
from src.services.migration import digest,key_digest,validate_backup
queue=redis.Redis.from_url(REDIS_URL)
def list_items():
 with Session() as s:return [record(r) for r in s.query(Run).order_by(Run.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  r=s.get(Run,item_id)
  if not r:raise HTTPException(404,'未找到对账运行')
  return record(r)
def create(data):
 with Session() as s:
  m=s.get(Mapping,data.mapping_id)
  if not m:raise HTTPException(404,'未找到字段映射')
  b=s.get(Batch,m.batch_id)
  r=Run(name=data.name,batch_id=b.id,mapping_id=m.id,status='queued',snapshot={'batch':record(b),'mapping':record(m),'retention_days':data.retention_days or ARCHIVE_RETENTION_DAYS})
  s.add(r);s.commit();s.refresh(r);result=record(r)
 try:queue.rpush('migration-runs',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Run,result['id']);r.status='failed';r.error='队列不可用，请创建新任务';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return result
def cancel(item_id):
 with Session() as s:
  r=s.query(Run).filter_by(id=item_id).with_for_update().first()
  if not r:raise HTTPException(404,'未找到对账运行')
  if r.status not in ('queued','running'):raise HTTPException(409,'运行已终止，无法取消')
  r.cancel_requested=True
  if r.status=='queued':r.status='cancelled'
  s.commit();return record(r)
def targets(item_id):
 detail(item_id)
 with Session() as s:rows=[r.payload for r in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
 return {'rows':rows,'count':len(rows),'checksum':digest(rows)}
def _archived_body(arch):
 try:token=archive.pin(arch['digest'])
 except KeyError:raise HTTPException(410,'归档正文已回收，无法读取')
 try:return json.loads(archive.read_body(arch['digest']))
 except KeyError:raise HTTPException(410,'归档正文缺失或已回收')
 finally:archive.unpin(token)
def backup(item_id):
 r=detail(item_id)
 if r['status']!='completed':raise HTTPException(409,'仅完成运行可以备份')
 res=r.get('result') or {}
 if res.get('backup'):return res['backup']
 arch=res.get('backup_archive') or {}
 if arch.get('status')=='archived':return _archived_body(arch)
 if arch.get('status')=='reclaimed':raise HTTPException(410,'备份保留期已过，归档正文已回收')
 raise HTTPException(404,'本次运行没有可用备份')
def restore(item_id,data):
 if data.from_archive:
  with Session() as s:
   r=s.get(Run,item_id)
   if not r:raise HTTPException(404,'未找到对账运行')
   if r.status!='completed':raise HTTPException(409,'仅完成运行可以恢复')
   arch=(r.result or {}).get('backup_archive') or {}
   if arch.get('status')!='archived':raise HTTPException(409,'该运行没有可恢复的归档正文')
   digest_value=arch['digest']
  try:token=archive.pin(digest_value)
  except KeyError:raise HTTPException(410,'归档正文已回收，无法从归档恢复')
  try:
   payload=json.loads(archive.read_body(digest_value))
   return _replace(item_id,payload,'archive')
  except KeyError:raise HTTPException(410,'归档正文缺失或已回收')
  finally:archive.unpin(token)
 return _replace(item_id,data.backup,'upload')
def _replace(item_id,payload,source):
 with Session() as s:
  r=s.query(Run).filter_by(id=item_id).with_for_update().first()
  if not r:raise HTTPException(404,'未找到对账运行')
  if r.status!='completed':raise HTTPException(409,'仅完成运行可以恢复')
  try:rows=validate_backup(payload,r.result['checksum'],item_id)
  except ValueError as e:raise HTTPException(422,str(e))
  before=s.query(TargetRow).filter_by(run_id=item_id).count()
  s.query(TargetRow).filter_by(run_id=item_id).delete()
  for i,row in enumerate(rows):s.add(TargetRow(run_id=item_id,ordinal=i,key_digest=key_digest(row),payload=row))
  s.flush()
  actual=[x.payload for x in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
  proof={'restored_at':datetime.now(timezone.utc).isoformat(),'source':source,'previous_count':before,'restored_count':len(actual),'checksum':digest(actual),'verified':digest(actual)==r.result['checksum']}
  if not proof['verified']:raise HTTPException(409,'恢复后校验失败，已回滚')
  r.recovery=(r.recovery or [])+[proof];s.commit();return proof
