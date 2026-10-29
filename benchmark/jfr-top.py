import sys,re,collections
# usage: jfr print --events jdk.ExecutionSample --stack-depth 40 f.jfr | python3 jfr-top.py [N]
inc=collections.Counter(); leaf=collections.Counter(); tot=0; blk=[]
for l in sys.stdin:
    if 'stackTrace = [' in l: blk=[]; continue
    if l.strip()==']':
        tot+=1; seen=set()
        for f in blk:
            m=re.sub(r'\(.*','',f)
            if m not in seen: inc[m]+=1; seen.add(m)
        if blk: leaf[re.sub(r'\(.*','',blk[0])]+=1
        blk=[]; continue
    if 'line:' in l: blk.append(l.strip())
N=int(sys.argv[1]) if len(sys.argv)>1 else 25
print('samples',tot); print('-- self'); 
for m,n in leaf.most_common(N): print(f'{100*n/tot:5.1f}% {m}')
print('-- inclusive (hydrangea / bench only)')
k=0
for m,n in inc.most_common(400):
    if ('hydrangea' in m or 'GrpcBench' in m) and k<N: print(f'{100*n/tot:5.1f}% {m}'); k+=1
