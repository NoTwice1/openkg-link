#coding utf-8
class HtmlOutputer(object):
    def __init__(self):
        self.datas = []
    def collect_data(self, data):
        if(data is None):
            return
        self.datas.append(data)

    def output_html(self):
        f = open('output.html','w')
        f.write("<html>")
        f.write("<head><meta charset='utf-8'></head>")
        f.write("<body>")
        f.write("<table>")
        for data in self.datas:
            f.write("<tr>")
            f.write("<td>%s</td>"%data['url'].encode('utf-8'))
            f.write("<td>%s</td>" % data['title'].encode('utf-8'))
            f.write("<td>%s</td>" % data['summary'].encode('utf-8'))
            f.write("</tr>")
        f.write("</table>")
        f.write("</body>")
        f.write("</html>")

        f.close()