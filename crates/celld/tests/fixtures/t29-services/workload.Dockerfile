FROM busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662
RUN mkdir -p /www/dir /www/cgi-bin \
    && echo -n ok > /www/index.html \
    && echo -n nested > /www/dir/index.html \
    && printf '#!/bin/sh\necho "Content-type: text/plain"\necho\nprintf "%%s" "$HTTP_HOST"\n' > /www/cgi-bin/host \
    && chmod +x /www/cgi-bin/host
CMD ["sh", "-c", "httpd -f -p 8080 -h /www & exec tcpsvd 0.0.0.0 9000 cat"]
LABEL celld.fixture=workload
