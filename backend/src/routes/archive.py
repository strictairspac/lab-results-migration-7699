from fastapi import APIRouter,Depends
from src.middlewares.auth import current,admin
from src.services import archive
from src.services.audit import log
router=APIRouter()
@router.get('/archive/status')
def status(u=Depends(current)):return archive.status()
@router.post('/archive/collect')
def collect(u=Depends(admin)):
 stats=archive.collect()
 if stats['reclaimed']:log(u.email,'archive.reclaimed',f"回收{len(stats['reclaimed'])}份过期正文，释放{stats['freed_bytes']}字节")
 else:log(u.email,'archive.collect','无可回收正文')
 return stats
