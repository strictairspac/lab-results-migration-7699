import hashlib,os,threading
from datetime import timedelta
import pytest
from fastapi import HTTPException
from src.config.database import Session
from src.models.archive import ArchiveRef
from src.models.run import Run
from src.schemas.business import RestoreInput
from src.services import archive,runs
from src.services.migration import migrate,make_backup
RULES=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in [('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),('单位','unit','text'),('时间','measured_at','datetime')]]
def sourced(tag='A',count=5):
 rows=[{'委托号':f'JS-{tag}-{i:03d}','检测项':'拉伸强度','结果':str(420+i),'单位':'MPa','时间':'2026-11-17T09:00:00+08:00'} for i in range(count)]
 result=migrate(rows,RULES,2)
 return result,make_backup(result,1,'batch-'+tag)
def sized(n):return {'payload':'x'*n}
def digest_of(body):return hashlib.sha256(archive.canonical_bytes(body)).hexdigest()
def expire_refs():
 with Session() as s:
  for r in s.query(ArchiveRef).all():r.retain_until=archive.now()-timedelta(days=1)
  s.commit()
def test_same_content_single_copy_separate_retention(mkrun):
 body=sized(200);a,b=mkrun(),mkrun()
 i1=archive.archive_backup(a,body,retention_days=10)
 i2=archive.archive_backup(b,body,retention_days=40)
 assert i1['digest']==i2['digest']
 st=archive.status()
 assert st['object_count']==1 and st['used_bytes']==i1['size_bytes']
 refs=st['objects'][0]['refs']
 assert {r['run_id'] for r in refs}=={a,b}
 assert len({r['retain_until'] for r in refs})==2
def test_concurrent_archive_same_content(mkrun):
 body=sized(5000);d=digest_of(body)
 ids=[mkrun() for _ in range(8)];errors=[]
 def work(rid):
  try:archive.archive_backup(rid,body)
  except Exception as e:errors.append(e)
 ts=[threading.Thread(target=work,args=(i,)) for i in ids]
 [t.start() for t in ts];[t.join() for t in ts]
 assert not errors
 st=archive.status()
 assert st['object_count']==1
 assert st['used_bytes']==len(archive.canonical_bytes(body))
 assert len(st['objects'][0]['refs'])==8
 assert archive.read_body(d)==archive.canonical_bytes(body)
def test_concurrent_archive_mixed_content(mkrun):
 bodies=[sized(300+i) for i in range(6)];errors=[]
 ids=[mkrun() for _ in range(12)]
 def work(rid,body):
  try:archive.archive_backup(rid,body)
  except Exception as e:errors.append(e)
 ts=[threading.Thread(target=work,args=(rid,bodies[i%6])) for i,rid in enumerate(ids)]
 [t.start() for t in ts];[t.join() for t in ts]
 assert not errors
 st=archive.status()
 assert st['object_count']==6
 assert st['used_bytes']==sum(len(archive.canonical_bytes(b)) for b in bodies)
def test_capacity_reject_then_reclaim_expired(mkrun,monkeypatch):
 monkeypatch.setattr(archive,'ARCHIVE_CAPACITY_BYTES',700)
 a,b,c=mkrun(),mkrun(),mkrun()
 ia=archive.archive_backup(a,{'tag':'a','pad':'x'*300})
 ib=archive.archive_backup(b,{'tag':'b','pad':'x'*300})
 size=ia['size_bytes'];assert size==ib['size_bytes'] and 2*size<=700<3*size
 with pytest.raises(archive.CapacityRejected) as e:archive.archive_backup(c,{'tag':'c','pad':'x'*300})
 assert '容量不足' in str(e.value)
 with Session() as s:
  r=s.get(Run,a);r.result={'backup_archive':ia};s.commit()
 expire_refs()
 with Session() as s:
  ref=s.query(ArchiveRef).filter_by(run_id=b).first();ref.retain_until=archive.now()+timedelta(days=1);s.commit()
 stats=archive.collect()
 assert stats['freed_bytes']==size and stats['used_bytes']==size
 with Session() as s:
  assert s.get(Run,a).result['backup_archive']['status']=='reclaimed'
 assert not os.path.exists(archive._path(ia['digest']))
 archive.archive_backup(c,{'tag':'c','pad':'x'*300})
 st=archive.status()
 assert st['object_count']==2 and st['used_bytes']==2*size
 again=archive.collect()
 assert again['reclaimed']==[] and again['used_bytes']==st['used_bytes']
def test_pin_protects_body_when_retention_expires_during_restore(mkrun):
 rid=mkrun();_,body=sourced();info=archive.archive_backup(rid,body)
 d=info['digest'];token=archive.pin(d)
 expire_refs()
 stats=archive.collect()
 assert stats['reclaimed']==[]
 assert archive.read_body(d)==archive.canonical_bytes(body)
 archive.unpin(token)
 stats=archive.collect()
 assert [x['digest'] for x in stats['reclaimed']]==[d]
def test_crash_after_body_written_before_register(mkrun,monkeypatch):
 rid=mkrun();body=sized(120);d=digest_of(body)
 real=archive._store_file;crash=[True]
 def bomb(digest,data):
  real(digest,data)
  if crash[0]:raise RuntimeError('power loss')
 monkeypatch.setattr(archive,'_store_file',bomb)
 with pytest.raises(RuntimeError):archive.archive_backup(rid,body)
 assert os.path.exists(archive._path(d))
 assert archive.status()['used_bytes']==0
 crash[0]=False
 info=archive.archive_backup(rid,body)
 assert info['digest']==d
 st=archive.status()
 assert st['object_count']==1 and st['used_bytes']==info['size_bytes']
 assert archive.read_body(d)==archive.canonical_bytes(body)
def test_restore_from_archive_and_legacy_upload(mkrun):
 result,body=sourced();rid=mkrun()
 info=archive.archive_backup(rid,body)
 with Session() as s:
  r=s.get(Run,rid);r.result={'checksum':result['checksum'],'backup_archive':info};s.commit()
 proof=runs.restore(rid,RestoreInput(from_archive=True))
 assert proof['verified'] and proof['source']=='archive' and proof['restored_count']==result['accepted_count']
 t=runs.targets(rid)
 assert t['count']==result['accepted_count'] and t['checksum']==result['checksum']
 proof2=runs.restore(rid,RestoreInput(backup=body))
 assert proof2['verified'] and proof2['source']=='upload'
def test_legacy_inline_backup_download_and_restore(mkrun):
 result,body=sourced('L');rid=mkrun()
 with Session() as s:
  r=s.get(Run,rid);r.result={'checksum':result['checksum'],'backup':body};s.commit()
 assert runs.backup(rid)==body
 proof=runs.restore(rid,RestoreInput(backup=body))
 assert proof['verified'] and proof['restored_count']==result['accepted_count']
def test_archived_download_and_reclaimed_410(mkrun):
 result,body=sourced('D');rid=mkrun()
 info=archive.archive_backup(rid,body)
 with Session() as s:
  r=s.get(Run,rid);r.result={'checksum':result['checksum'],'backup_archive':info};s.commit()
 assert runs.backup(rid)==body
 expire_refs();archive.collect()
 with pytest.raises(HTTPException) as e:runs.backup(rid)
 assert e.value.status_code==410
 with pytest.raises(HTTPException) as e:runs.restore(rid,RestoreInput(from_archive=True))
 assert e.value.status_code==409
def test_app_routes_registered():
 from src.main import app
 paths={r.path for r in app.routes}
 assert '/archive/status' in paths and '/archive/collect' in paths
