package org.apache.hadoop.hbase.rest.filter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GzipFilter implements Filter {
  private Set<String> mimeTypes = new HashSet<String>();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String s = filterConfig.getInitParameter("mimeTypes");
    if (s != null) {
      StringTokenizer tok = new StringTokenizer(s, ",", false);
      while (tok.hasMoreTokens()) {
        mimeTypes.add(tok.nextToken());
      }
    }
  }

  @Override
  public void destroy() {
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse rsp,
      FilterChain chain) throws IOException, ServletException {
    HttpServletRequest request = (HttpServletRequest)req;
    HttpServletResponse response = (HttpServletResponse)rsp;
    String contentEncoding = request.getHeader("content-encoding");
    String acceptEncoding = request.getHeader("accept-encoding");
    String contentType = request.getHeader("content-type");
    if ((contentEncoding != null) &&
        (contentEncoding.toLowerCase().indexOf("gzip") > -1)) {
      request = new GZIPRequestWrapper(request);
    }
    if (((acceptEncoding != null) &&
          (acceptEncoding.toLowerCase().indexOf("gzip") > -1)) ||
        ((contentType != null) && mimeTypes.contains(contentType))) {
      response = new GZIPResponseWrapper(response);
    }
    chain.doFilter(request, response);
    if ((response instanceof GZIPResponseWrapper)) {
      ((GZIPResponseStream)response.getOutputStream()).finish();
    }
  }
}