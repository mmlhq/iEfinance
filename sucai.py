

# def uncompileStr(code):
#     code = unescape(code);
#     var c = String.fromCharCode(code.charCodeAt(0) - code.length);
#     for (var i = 1; i < code.length; i++) {
#         c += String.fromCharCode(code.charCodeAt(i) - c.charCodeAt(i - 1));
#     }
#     return c;

#escape 解码

import urllib.parse

def unescape(str):
    st = str.replace('%u', '\\u')
    code =urllib.parse.unquote(st.encode().decode('unicode-escape'))
    return code

