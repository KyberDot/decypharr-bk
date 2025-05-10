package debrid

import (
	"bytes"
	"fmt"
	"github.com/sirrobot01/decypharr/internal/request"
	"net/http"
	"os"
	"path"
	"time"
)

func (c *Cache) refreshParentXml() error {
	parents := []string{"__all__", "torrents"}
	torrents := c.GetListing()
	clientName := c.client.GetName()
	for _, parent := range parents {
		if err := c.refreshFolderXml(torrents, clientName, parent); err != nil {
			return fmt.Errorf("failed to refresh XML for %s: %v", parent, err)
		}
	}
	return nil
}

func (c *Cache) refreshFolderXml(torrents []os.FileInfo, clientName, parent string) error {
	buf := c.xmlPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.xmlPool.Put(buf)

	// static prefix
	buf.WriteString(`<?xml version="1.0" encoding="UTF-8"?><D:multistatus xmlns:D="DAV:">`)
	now := time.Now().UTC().Format(http.TimeFormat)
	base := fmt.Sprintf("/webdav/%s/%s", clientName, parent)
	writeResponse(buf, base+"/", parent, now)
	for _, t := range torrents {
		writeResponse(buf, base+"/"+t.Name()+"/", t.Name(), now)
	}
	buf.WriteString("</D:multistatus>")

	data := buf.Bytes()
	gz := request.Gzip(data, &c.gzipPool)
	c.PropfindResp.Store(path.Clean(base), PropfindResponse{Data: data, GzippedData: gz, Ts: time.Now()})
	return nil
}

func writeResponse(buf *bytes.Buffer, href, name, modTime string) {
	fmt.Fprintf(buf, `
<D:response>
  <D:href>%s</D:href>
  <D:propstat>
    <D:prop>
      <D:resourcetype><D:collection/></D:resourcetype>
      <D:displayname>%s</D:displayname>
      <D:getlastmodified>%s</D:getlastmodified>
      <D:getcontenttype>httpd/unix-directory</D:getcontenttype>
      <D:getcontentlength>0</D:getcontentlength>
      <D:supportedlock>
        <D:lockentry><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockentry>
      </D:supportedlock>
    </D:prop>
    <D:status>HTTP/1.1 200 OK</D:status>
  </D:propstat>
</D:response>`, href, name, modTime)
}
