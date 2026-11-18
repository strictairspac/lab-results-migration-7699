# Schema v2 · 库外归档
新增 archive_objects(digest PK, size_bytes, created_at)、archive_refs(run_id UNIQUE FK→runs, digest, retain_until)、archive_pins(token PK, digest, expires_at)。runs.result 不再保存新备份正文，改写 backup_archive={status,digest,size_bytes,retain_until} 指针；容量拒绝/异常时保留库内正文并记录 reason；回收后状态置 reclaimed。

备份正文按规范JSON的SHA-256命名存放在共享卷 objects/ 下，相同内容全库只有一份。额度=archive_objects.size_bytes合计，登记与回收在同一事务内删建行，文件先写后登记、先删行后扫文件，中途停机只留孤儿文件，重试不重复占用额度。PG会话级advisory锁串行化登记/回收/固定，恢复与下载用pin租约防止误删正在读取的正文；引用全部到期且无固定才可回收。
