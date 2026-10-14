import {DurableObject} from "cloudflare:workers";
const childSource = `
import {DurableObject} from "cloudflare:workers";
let retained = null;
function bytes(size) {
  const value = new Uint8Array(size);
  for (let i=0; i<value.length; i+=4096) value[i]=7;
  return value;
}
function count(value) {
  let checksum=0;
  if(value) for(let i=0;i<value.length;i+=4096) checksum+=value[i];
  return {bytes:value?.length??0,checksum};
}
export class F extends DurableObject {
  hold() { this.retained=bytes(4*1024*1024); return count(this.retained); }
  status() { return count(this.retained); }
  release() { this.retained=null; return count(this.retained); }
}
export default {
  fetch(request) {
    const path=new URL(request.url).pathname;
    if(path==="/hold") retained=bytes(8*1024*1024);
    if(path==="/release") retained=null;
    return Response.json(count(retained));
  }
};
`;
export class Counter extends DurableObject {
  constructor(ctx,env) { super(ctx,env); this.worker=null; this.generation=0; }
  child() {
    return this.worker ??= this.env.LOADER.get("memory-"+(++this.generation),()=>({
      compatibilityDate:"2026-03-01", mainModule:"main.js",modules:{"main.js":childSource}
    }));
  }
  facet(id) {
    const cls=this.child().getDurableObjectClass("F");
    return this.ctx.facets.get(id,()=>({class:cls}));
  }
  async fetch(request) {
    const path=new URL(request.url).pathname;
    if(path==="/hold") {
      const child=await (await this.child().getEntrypoint().fetch("http://child/hold")).json();
      const first=await this.facet("first").hold();
      const second=await this.facet("second").hold();
      return Response.json({loaded:true,child,first,second});
    }
    if(path==="/release") {
      if(this.worker) {
        await this.facet("first").release();
        await this.facet("second").release();
        await (await this.child().getEntrypoint().fetch("http://child/release")).json();
      }
      return Response.json({released:true});
    }
    if(path==="/teardown") {
      if(this.worker) {
        await this.ctx.facets.delete("first");
        await this.ctx.facets.delete("second");
        this.worker.dispose();
        this.worker=null;
      }
      return Response.json({loaded:false});
    }
    if(!this.worker) return Response.json({loaded:false});
    const child=await (await this.worker.getEntrypoint().fetch("http://child/status")).json();
    return Response.json({loaded:true,child,
      first:await this.facet("first").status(),second:await this.facet("second").status()});
  }
}
export default {
  fetch(request,env) {
    const path=new URL(request.url).pathname;
    return env.COUNTER.getByName("memory").fetch("http://object"+path);
  }
};
