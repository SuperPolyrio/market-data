import hashlib,json
from pathlib import Path
out=Path(__file__).parent;results=[]
for node in ['P-0000','Q-0001']:
 path=out/(node+'-probe-events.json');events=json.loads(path.read_bytes());connections={};late=0;verified=0;errors=[];recoveries=[]
 for event in events:
  body=event['body'];conn=body.get('connection');state=connections.setdefault(conn,{'pending':None,'ambiguous':False,'good':0,'failed':None,'expect':None})
  if event['kind']=='PING_SEND':
   if body['probe']:
    if state['pending'] is not None:errors.append('OVERLAPPING_NAMED_PROBE')
    state['pending']=event
   elif state['pending'] is not None:state['ambiguous']=True
  elif event['kind']=='RAW' and state['pending'] and not state['ambiguous']:
   millis=(body['recv_mono_ns']-state['pending']['body']['send_enqueued_mono_ns'])/1e6
   state['good']=state['good']+1 if millis<=2000 else 0
   state['expect']=(millis,state['good']>=3,event['offset'])
   if millis>2000:state['failed']={'ping_offset':state['pending']['offset'],'pong_offset':event['offset'],'rtt_ms':millis};late+=1
   state['pending']=None
  elif event['kind']=='TRANSPORT_QUALITY' and body.get('rtt_ms') is not None:
   expected=state['expect']
   if expected is None:continue
   if abs(expected[0]-body['rtt_ms'])>0.000001 or expected[1]!=body['healthy']:errors.append({'offset':event['offset'],'expected':expected,'actual':body})
   verified+=1
   if body['healthy'] and state['failed']:
    recoveries.append({'connection':conn,'failed_probe':state['failed'],'requalified_record':event['offset'],'last_good_pong_record':expected[2],'consecutive_good':state['good'],'stream':event['stream']});state['failed']=None
   state['expect']=None
 results.append({'node':node,'input_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'late_unique_responses':late,'verified_quality_decisions':verified,'errors':errors,'same_connection_requalifications':recoveries})
receipt={'contract':'RTT<=2000ms; late unique response fails its own probe; three new good probes required; overlaps stay ambiguous','results':results,'status':'PASS' if not any(r['errors'] for r in results) and any(r['same_connection_requalifications'] for r in results) else 'FAIL'}
(out/'probe-acceptance.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt,indent=2))
