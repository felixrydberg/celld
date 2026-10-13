FROM busybox:1.36
RUN mkdir -p /www/dir /www/cgi-bin \
    && echo -n ok > /www/index.html \
    && echo -n nested > /www/dir/index.html \
    && printf '#!/bin/sh\necho "Content-type: text/plain"\necho\nprintf "%%s" "$HTTP_HOST"\n' > /www/cgi-bin/host \
    && chmod +x /www/cgi-bin/host
CMD ["sh", "-c", "httpd -f -p 8080 -h /www & exec tcpsvd 0.0.0.0 9000 cat"]
