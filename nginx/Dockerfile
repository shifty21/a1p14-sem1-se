FROM alpine:latest

MAINTAINER ykhedar (yateender.khedar@yateenderk@gmail.com)

# Install Nginx.
RUN \
  apk update && \
  apk add nginx && \
  rm -rf /var/lib/apt/lists/* && \
  adduser -D -g 'www' www && \
  chown -R www:www /var/lib/nginx

RUN mkdir -p /run/nginx
# Define mountable directories.
VOLUME ["/etc/nginx/sites-enabled", "/etc/nginx/certs", "/etc/nginx/conf.d", "/var/log/nginx", "/var/www/html"]

# Copy Config file.
COPY nginx.conf /etc/nginx/
RUN cat /etc/nginx/nginx.conf

# Create directory for static content.
RUN mkdir -p /var/www/nginx
COPY www /var/www/nginx/
RUN ls -la /var/www/nginx

# Working directory.
WORKDIR /etc/nginx

# Start nginx server.
CMD ["nginx"]

# Expose ports.
EXPOSE 80
EXPOSE 443
