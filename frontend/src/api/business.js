import {api,post} from './client.js';
export const batches={list:()=>api('/batches'),save:(d,id)=>post('/batches'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const mappings={list:()=>api('/mappings'),save:(d,id)=>post('/mappings'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const runs={list:()=>api('/runs'),run:d=>post('/runs',d),cancel:id=>post('/runs/'+id+'/cancel',{}),targets:id=>api('/runs/'+id+'/targets'),restore:(id,backup)=>post('/runs/'+id+'/restore',{backup}),restoreArchive:id=>post('/runs/'+id+'/restore',{from_archive:true})};
export const archive={status:()=>api('/archive/status'),collect:()=>post('/archive/collect',{})};
