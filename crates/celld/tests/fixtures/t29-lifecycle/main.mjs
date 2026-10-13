export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === '/slow') {
      await env.CACHE.put('slow-started', env.LABEL);
      await new Promise(resolve => setTimeout(resolve, 20000));
      return new Response('slow ' + env.LABEL);
    }
    if (path === '/slow-started') {
      return new Response(await env.CACHE.get('slow-started') || 'pending');
    }
    return new Response('hello ' + env.LABEL);
  }
};
