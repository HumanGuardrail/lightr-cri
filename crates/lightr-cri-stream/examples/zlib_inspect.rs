//! Scratch: dump the first bytes of the server's SYN_REPLY zlib segment to
//! check whether the zlib header sets the FDICT bit (CMF/FLG) the way
//! moby/spdystream's reader expects. Dev-only.

use flate2::{Compress, Compression, FlushCompress};

fn main() {
    // Mirror HeaderCompressor::new + compress(count=0 block).
    let dict: &[u8] = lightr_cri_stream_dict();
    let mut z = Compress::new(Compression::default(), true);
    z.set_dictionary(dict).unwrap();

    let block = [0u8, 0, 0, 0]; // count=0 header block
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    let mut consumed = 0usize;
    loop {
        let bi = z.total_in();
        let bo = z.total_out();
        z.compress(&block[consumed..], &mut buf, FlushCompress::Sync)
            .unwrap();
        let read = (z.total_in() - bi) as usize;
        let wrote = (z.total_out() - bo) as usize;
        consumed += read;
        out.extend_from_slice(&buf[..wrote]);
        if consumed >= block.len() && wrote < buf.len() {
            break;
        }
        if read == 0 && wrote == 0 {
            break;
        }
    }
    println!("len={}", out.len());
    print!("bytes=");
    for b in &out {
        print!("{b:02x} ");
    }
    println!();
    // zlib header analysis
    if out.len() >= 2 {
        let cmf = out[0];
        let flg = out[1];
        let fdict = (flg & 0x20) != 0;
        println!("CMF={cmf:02x} FLG={flg:02x} FDICT={fdict}");
        if fdict && out.len() >= 6 {
            println!(
                "DICTID={:02x}{:02x}{:02x}{:02x}",
                out[2], out[3], out[4], out[5]
            );
        }
    }
    // tail
    let n = out.len();
    if n >= 4 {
        println!(
            "tail={:02x} {:02x} {:02x} {:02x}",
            out[n - 4],
            out[n - 3],
            out[n - 2],
            out[n - 1]
        );
    }
}

fn lightr_cri_stream_dict() -> &'static [u8] {
    // Inline copy of the canonical dict (same bytes as src/spdy/dict.rs).
    b"\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-charset\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\x0daccept-ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\x0dauthorization\x00\x00\x00\x0dcache-control\x00\x00\x00\x0aconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10content-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\x0dcontent-range\x00\x00\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\x00\x00\x11if-modified-since\x00\x00\x00\x0dif-none-match\x00\x00\x00\x08if-range\x00\x00\x00\x13if-unmodified-since\x00\x00\x00\x0dlast-modified\x00\x00\x00\x08location\x00\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0bretry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\x0auser-agent\x00\x00\x00\x04vary\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\x0aset-cookie\x00\x00\x00\x0akeep-alive\x00\x00\x00\x06origin100101201202205206300302303304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative Information204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMTchunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,text/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-8859-1,utf-,*,enq=0."
}
