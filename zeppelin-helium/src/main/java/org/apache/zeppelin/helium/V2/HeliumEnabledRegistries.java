/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.helium.V2;

import java.util.HashSet;
import java.util.Set;

public class HeliumEnabledRegistries {
  
  private static final Set<HeliumRegistry> registries = new HashSet<>();

  public static Set<HeliumRegistry> getRegistries() {
    return new HashSet<>();
  }

  static {

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-column-chart",
            "The Ultimate Column Chart for Apache Zeppelin using \u003ca href src\u003d\"http://www.highcharts.com/\"\u003ehighcharts\u003c/a\u003e",
            "ultimate-column-chart@0.0.2",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Line-Chart\" src\u003d\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAB1ElEQVRoQ+1azUrDQBCebFMPtWlJPejRd1AQvFihIPgMpVS0dw9SFC/tQZDiG7RgKX0JpWC9in2HHlXQHFJyaDcb2dAFtY2bkByWMntbMkP225nv29kfDVakaSuCA5QCUmnXG0DACzu5jLHnfu1uyO3VAtKpe0YmM9H1FJWBsSdOlrruTa/WaigJpLSzC1vmhgwHPI5e4OPLaiIQ6VTFMKh06h5GJMYEJu6KEVFRfpEjItHL7YsiIeQgSuL3TlvNKPYy20Q4wssDQshV3lh3ZD+k1NVtx8n2zlqJVgaJASnkjPPjvf28DMib9QmD0SsgkICZwoj8lF/OEUwtGalCfsfUwtRC+f2fLMgR5MgSjvhFp0aeQiqtb8Y8diiOc3hfidQSQPh+IkzjtZrSQMqlozA4oD94QCB8pgLPteLWWiK1MCKqkR0jghH5cxqPZJ+vGqha8xsn5UoUVC1ULVQtgGVHpqhaqFoB19O4squ4sqfTqeuCkdNle9XpjII1sX/djwiyb5qmzN3//m5ZS7e6ZtaAtbR0CGDZNkxndPHlgz8QIMVQo5gbiecTvFu9v9xmlFWj+BOddLsnt2Ph4z+qidAYsKGSj2oiYFgwTfQOMM5A4vp+A403Sm9lQCHKAAAAAElFTkSuQmCC\" \u003e",
            "2018-01-23T03:11:59.022Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-heatmap-chart",
            "The Ultimate Heatmap Chart for Apache Zeppelin using \u003ca href src\u003d\"http://www.highcharts.com/\"\u003ehighcharts\u003c/a\u003e",
            "ultimate-heatmap-chart@0.0.2",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Gantt-Chart\" src\u003d\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAEHklEQVRoQ+1aXWgcVRT+dmbcmexmN5N2szW1abNJTahdI0TpQ6RJQKggNqCoCP3Blz70pSg0Gh8KBUGjKSh96YMvohRERaUVQUFIUxpQasCY/iQ22Wy3ZOtmNzvJ/mR2M7ORu0iImzk7q01/DHde5+y59zv3nO+bc+46sEEexwbBgRUgx0+fe12As8cOmMNMhx3G/LSd3b15X7jQ33tkkK21AqT39PeDglzT5fFWLVObSKd0h1fK5qrF1CJl4/V4pOxixjSMgqUfSRIcriq3uJBKGZQPp+wUJFF0ZLOLJmUTm9PchmG829975OQaIKp/a9fe7ifIYA4P/Y7mOhkvdbfQNldCCDbWw+tWLG0WMjrGpqPo2B0gfURiScxn9aIf6jlz9rx+M/rn+xwIP5G/c4SnVkmx8BphAfnfsJao1HapajVJeYlECi5ZgMf1EGljmgXs2bUZvlpr+mXvM3qepGfmOJc3YBQKcCtOcp3LI2P58anwe2vo980Pv/xkU+2mg807tonUr6+ENcjuKvh8NeQC49ci2N+xDYF6r6UN22RsPo2GOpX0wQhBNwz4a+ig/jj0S+7GdKR/DZC+gY9Pbq/f8tbRA/utQwng1BcjUP0+tO7aTm7i3NeXcOjZFrQ11d0fQeRASuLOT2RVQO7oW4unFk+tu/QZz1LrsUeb+pp3BmSKWy+OJhDXcuvS/L3QufWO/EzeCOWu/jFlrSOBhvq+V59/hgQyMhGFIApQq13kJj77YQLBtgC8qttaR7QMxkZDOHH4SdJHTEsV1T/w8GbS5vPvfsqFItH/Joiss6txKWjw15IL9J65hI7OIKn+8fg8hofGMHD06bvTIVbCWhzIqtjzE6m0seKpVRKpB6LY7eg3dDtRbHb8qodknHc+/RW72wJQiV4ikVjA9athHN7XSvrQ0lmYBRPtLfRcqyz92gliJQr2zdBMJWa2Nj5Vxt42WkfKCqJdY1UJ/Q7bTBpHp2bBRLPnRVpHxq/dhBaL4/gr7SRgcoqyXsXOgayKPT8RFgyeWiXlyGtkPWtkZ2ND377OPWQ/wgZriiSVnRJGZrXiYE12SpbUGYou4PzwrbKzsdszCRj5HIIBul2YDM8Yc3Pxsx+88fJrbKGVqzdWI61NO95+qj1IzilZsyMJArlJ5pBNCZn6i6JgCSSe1PHz9QQEkRxoIp3NI79UgEo0Z8yxpmVg6tqFgWPPda8Bci8EsZL7ka8GJzA5m0NH5+OkIF4c/A1abIYDsW117ViLn0hJkvHUYgFZj69fnlr/JrVkp/PEI1t81gIAQFGUYue2lF8iadHtdkHXdbArNquH6Qvzk8lkSR/JvAtJXUG5a0BNS8PUk5b0y4SlKC73+ylIauOy6G6028cyjG9PHev56B+CaPejB/39hvm/1l9zuh9+d3X4DAAAAABJRU5ErkJggg\u003d\u003d\" \u003e",
            "2018-01-23T03:46:12.597Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "zeppelin-highcharts-bubble",
            "Bubble Chart for Apache Zeppelin using highcharts.js",
            "zeppelin-highcharts-bubble@0.0.2",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003csvg id\u003d\"Слой_1\" style\u003d\"enable-background:new 0 0 512 512;\" version\u003d\"1.1\" viewBox\u003d\"0 0 512 512\" xml:space\u003d\"preserve\" xmlns\u003d\"http://www.w3.org/2000/svg\" xmlns:xlink\u003d\"http://www.w3.org/1999/xlink\"\u003e\u003cstyle type\u003d\"text/css\"\u003e .st0{fill:none;stroke:#FFFFFF;stroke-width:7;stroke-linecap:round;stroke-linejoin:round;stroke-miterlimit:10;} .st1{fill:#248139;stroke:#FFFFFF;stroke-width:10;stroke-linecap:round;stroke-linejoin:round;stroke-miterlimit:10;} .st2{fill:#F5BE40;stroke:#FFFFFF;stroke-width:10;stroke-linecap:round;stroke-linejoin:round;stroke-miterlimit:10;} .st3{fill:#32A9B8;stroke:#FFFFFF;stroke-width:10;stroke-linecap:round;stroke-linejoin:round;stroke-miterlimit:10;} .st4{fill:#CF3722;stroke:#FFFFFF;stroke-width:10;stroke-linecap:round;stroke-linejoin:round;stroke-miterlimit:10;} \u003c/style\u003e\u003cg\u003e\u003cpolyline class\u003d\"st0\" points\u003d\"33.9,54.6 33.9,485.6 473,485.6  \"/\u003e\u003ccircle class\u003d\"st1\" cx\u003d\"100.5\" cy\u003d\"414.8\" r\u003d\"35.7\"/\u003e\u003ccircle class\u003d\"st2\" cx\u003d\"186.4\" cy\u003d\"293.1\" r\u003d\"55.1\"/\u003e\u003ccircle class\u003d\"st3\" cx\u003d\"314.7\" cy\u003d\"379.1\" r\u003d\"32.2\"/\u003e\u003ccircle class\u003d\"st4\" cx\u003d\"322.7\" cy\u003d\"141.9\" r\u003d\"66\"/\u003e\u003ccircle class\u003d\"st1\" cx\u003d\"421\" cy\u003d\"283\" r\u003d\"40.7\"/\u003e\u003ccircle class\u003d\"st3\" cx\u003d\"123.7\" cy\u003d\"152.1\" r\u003d\"23.2\"/\u003e\u003c/g\u003e\u003c/svg\u003e",
            "2017-02-14T12:30:44.199Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-line-chart",
            "The Ultimate Line Chart for Apache Zeppelin using \u003ca href src\u003d\"https://www.amcharts.com/\"\u003eamcharts\u003c/a\u003e",
            "ultimate-line-chart@0.0.1",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Line-Chart\" src\u003d\"data:image/png;base64,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\u003d\u003d\" \u003e",
            "2017-04-12T09:57:45.479Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "zeppelin-highcharts-scatterplot",
            "Scatter plot for Apache Zeppelin using highcharts.js",
            "zeppelin-highcharts-scatterplot@0.0.2",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg class\u003d\"icon icons8-Scatter-Plot\" src\u003d\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAFUUlEQVRoQ+2ZQWwbVRCGZ9brCEQtYlVtaCuSNEUQSmldpQK1QiVGiAsHnAsHVAdH2L02kbAFp7S3ykaquWKjOHbFgQtG4oQq4iKEBCSq20uTigY3QKFBEIcUtarXO+g9e53tetfZXTtOLHUvVpz3duZ78+Z/88Z4+sMLBNVHJvAmz0/klL+302dg6oP+crk8iQA+AOwGojwgpNLB2MfMT+wEkHc+CXtEAWY4gOYhgGwmGB3pCBB/MpJHgCNGO4QIJrY9SCUaeEWBGHp2ENyuHXBt8SYsr6xUvibKb3uQU4n3hwUUZpi/u91ueGPoJe77nyt/w6W52VqQOgqkSxTB98pJ6BKdcH3pFszdmK9GBFa3DISrkCSfASQPABYRKKcokDoXAlPj3bLUVQCEJ9n3DMbtcsEdZVtVBk9vCYj/03AACafqkpcoL4iOkdTY+YL6f4bjeX7AqiAKnraDaJO3Xk4plwnGvNrvOYyMcSUylRyny2WC8c9Ox9qf7KPJSAoA3mWOOEURnu/tg7v378Hi7ds132WSvRdDH+kezCz52UCZsMgAlEltj4g/Ec4h4qvMgZNHPNC7q4f78tUP30NxbU1J3nPpUPSs0bmh9/2Wghx/4RAc2LOvM0HUW4sp0MCefXxr/fbXcm2hJZmOqreNmci0PSJMdmVJZgUfl9O6ZCe6nAnFeB5YedoOwpzjyoWY08IwFXKIJV9qLF60AsHGNgXCDqsyOXkxV5Zg1cp24HNlpw9l7OcqBHJOT6nM2rANMpqInAWESfXKEVDO4SiN2FlRvQhYsWELZDQZjgPgGd3w89O55G0WxqoNyyA8WcvyLwrEE48/Bk7RuX4G8BMXJjKhaNzqPlfG27FhGcSfiIwjwgVmlEG8+fIJXo1eW/yZ3xHYw5LWjvIoIHZsWAZR71uj+0GzIHZsWAY5lYz4BIAvlNU7fvAQj8zcwgKs3K2UGET0ZSYU89ndWnZsWAbhclh25hGwz8hRvaKPz5O6JhEpoDQRmMqVZZjQyrYdG5ZBmPP8+glC1uB0nk4HowE1ZOVy5JwBRI/OWV6UZPBqYazasAXCnKnc8MpxBBxmQARwFZDimfdirEx/6NE7DzRDCulgdL92nhUbtkGs7H9/MlxQtuLA3r1w4uCLdc0DO4Wi2oe2gIwmI7Vu5utDx+Ap907uw9dzP9ZaOo0uU2YWrS0g6ogMPt0Hx54bhH/W/oVLcz/BA0nifm46CKtUHQKwcoQXdwBQcDgc57QNgkar1rDc4P0DupUJxpT3mwlA3ZiGETHuXpCu0hh5sJGcygAjF4PRrC2C6iRDEG29oyebgqO032xxWDlHnClEfEt5F4sEAY43C8HeZwii3g6s28Hu1yVJgtmFef7JHkIa05PbRivLgCRJ9Gi7IM1EoyGIuttxeOAAHB54htuavXEd5peWKnYJLHc7mnXYaL5hRPyJcFbZBor2s5fkrl5ZbxR0Bsh6uc4A3DtcfDGUwrCRZPI7uWM9FwRBmLaicnai1li1GvzAYlSqG0mtnXyyAtQQpFrrMKXhncGa2hh0OzaqqVohs5ZzRD2hWonyXpNElDXqlowmIkWlIu52uaB31264+cfv8N+9+1VxoHw6FDtqZaXNjm1ZiaL+ZYkZf3v4NX4FZl3E7Hff1vxJB6No1jkr4zYd5IFUgs9z33QOCPNUXeX2uN3Q494Jvy7fWb8Ct6CmaipHzIZ4o2TfTOVq2dZSYNUHqWYB6q7AZhfIzLiWgzCj1S6Ij4j6EbEgk5wy+gXKjJNmxmwKiBnDrR7zCKTVK9rs+x6KSLMv28r5j0C2cvX1bP8Pf6sbRhuKm9EAAAAASUVORK5CYII\u003d\" width\u003d\"15\" height\u003d\"15\" /\u003e",
            "2017-02-14T12:17:22.411Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-dual-column-chart",
            "The Ultimate Column Chart for Apache Zeppelin using \u003ca href src\u003d\"http://www.highcharts.com/\"\u003ehighcharts\u003c/a\u003e",
            "ultimate-dual-column-chart@0.0.5",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Line-Chart\" src\u003d\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAB1ElEQVRoQ+1azUrDQBCebFMPtWlJPejRd1AQvFihIPgMpVS0dw9SFC/tQZDiG7RgKX0JpWC9in2HHlXQHFJyaDcb2dAFtY2bkByWMntbMkP225nv29kfDVakaSuCA5QCUmnXG0DACzu5jLHnfu1uyO3VAtKpe0YmM9H1FJWBsSdOlrruTa/WaigJpLSzC1vmhgwHPI5e4OPLaiIQ6VTFMKh06h5GJMYEJu6KEVFRfpEjItHL7YsiIeQgSuL3TlvNKPYy20Q4wssDQshV3lh3ZD+k1NVtx8n2zlqJVgaJASnkjPPjvf28DMib9QmD0SsgkICZwoj8lF/OEUwtGalCfsfUwtRC+f2fLMgR5MgSjvhFp0aeQiqtb8Y8diiOc3hfidQSQPh+IkzjtZrSQMqlozA4oD94QCB8pgLPteLWWiK1MCKqkR0jghH5cxqPZJ+vGqha8xsn5UoUVC1ULVQtgGVHpqhaqFoB19O4squ4sqfTqeuCkdNle9XpjII1sX/djwiyb5qmzN3//m5ZS7e6ZtaAtbR0CGDZNkxndPHlgz8QIMVQo5gbiecTvFu9v9xmlFWj+BOddLsnt2Ph4z+qidAYsKGSj2oiYFgwTfQOMM5A4vp+A403Sm9lQCHKAAAAAElFTkSuQmCC\" \u003e",
            "2017-07-06T18:57:35.252Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-area-chart",
            "The Ultimate Area Chart for Apache Zeppelin using \u003ca href src\u003d\"https://www.amcharts.com/\"\u003eamcharts\u003c/a\u003e",
            "ultimate-area-chart@0.0.1",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Line-Chart\" src\u003d\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAE+klEQVRoQ+2Za0xbZRjH/+ecnlLsdSDlUimXshawY+UyNnQh20e5DRMTNfGyL0tMZoImumwlBvjg1MVkS9wXs5ip07jExEywzOiHwdDEoVGRD2b6QRkbjkLpnZa2p8ecauc6aPv2Rujk/fq+7/M8v+f/Pu/lHAr3SaPuEw7kNUiX2WIaP9n9syBGXoL0HB87wNDMBR78e6Mnu4fzDqR/+IqKW/MOURT1EsMw4DhuJO9Aek+M99M0LrAsKzYY9OK5uRtwuVzZAYlkKLB6muL5CZpnJi+9+dif2d48+o9fruYZnOX5cLdWWwnNQxqIGAYzM7PZA+kbHJ8Ss+w+AKFAICChGfo6F+IuA/SlsTe6JjOF6jNbBiiKel0ikbD1DQaxTCq9YzJrIH1myxmaZo62tTaLCiQF8Hi9cDqcWLIu+90et4SiKC9ATfBh7tNU1RJ2IxFFvcOA3ltVo2U1mop1OckKSK/ZcpgCzje3mHB3lqLeQhwXgbItL8PucPpTUavPbBkCMCyXy0MN9YZIkjZqGYNEsgX8pNfvRGmpmmj1kKgV3VIpmi6rr9eLiouLEtrOCEQobj6wekOtLpHrDXoiiHsHxVMrzIUNpeoS1NbpIsWcrGUEcmhw/JfCQklja2tLck/JIvm3P6qWVCqFSqUknIX0d61es+W8iKafbd/XzpBkjDiiNAempUiy4k4zloympQySTnFnFCHh5JRA/jlV+dny8jKZTldL6GJzhhGDRHaooP9bmUyqN5maRJsTHrkXYpBDr335McswT7S2tYi3QnHfi0gE0nviC+Ga/NZuU1PM/YY8X7kfmRREOGFpmr6Sysmd+7DXe0gIIuxQLEVNbcXiXre0Zn/EanD+3c8Gn39B6Lvz1BWKG5z/O6VSWWt8uJElyTLH++FYnYVCogfLkJ/KJLbjjQlyTtx2XYXL9yv8LtXkxZePHYgB6TNbhlmWfbVtT+sDJMXt8v+Gm/ZRcOFAxGeBSAFZgQEySVUELNtN8GfzXoPHP48HlUXwra3CviTbGEQmlQ40t5hUiYIQVFh0TsHm/R67anTYrauD1+/DvNWKBdsKFmzWyHRF4U7ICqozUiuquM1zDWshF2rLK1CvrUaRXI6vfpjGzXkmPRDP2hwWHGOgmSAOmpojBjdq80tWLK7YMGddgm/Nl7JawvJZck/D6ZsBKB4NWi0aqqogFv234tMCuVuF+kotmnR1MUYTKZiKWkKibN5puHy/QylVoLFaC125ZkPzKYP4gotYcHwOnvJiv9GI0h2JHzzJaiOeWu616wiEXKgp06BOU5HUT0ogVvc3WHRdRUWxGvt3GYlVSAYT7Y+q9deKDUVyBeo0GkglhUTTiUAEFW7ZRxEMO/Co0YjKErInLVEEWRqUFERrCKoWHF/nTIUsccTftR4fuXhGXvrHUZHYI9rTYIhbZNkKJFM7cRV56vSpCXmxp7OrvZ0iXaeZBpPJ/IQg6jKus6ejPS++0CcEKSnjOnvzHeTpsyPvlxQzT/Z0dEgykXyz5gqK3Lod+OCTF4cOx1wanzt3bHiHQj7QvfeRhHetzQo0mR8BxGq3j3x45FTsj55tkGSpy1H/tiI5SmzaZrcVSTt1OZq4rUiOEpu22f+HIizLDBbJFVvug/VGstndbgSCofUn+zPnXjlAg4587MqXFkZ44qMjb0/E3LXyJfh4cebF24MkyX8DpFkiYFDvs/EAAAAASUVORK5CYII\u003d\" \u003e",
            "2017-04-12T09:58:18.693Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "zeppelin-bubblechart",
            "Animated bubble chart",
            "zeppelin-bubblechart@0.0.4",
            "BSD-2-Clause",
            "\u003csvg viewBox\u003d\u00270 0 70 70\u0027\u003e\u003ccircle cx\u003d\u002726\u0027 cy\u003d\u002728\u0027 stroke\u003d\u0027black\u0027 style\u003d\u0027stroke-width: 1px;\u0027 r\u003d\u002725\u0027 fill\u003d\u0027seagreen\u0027/\u003e\u003ccircle cx\u003d\u002742\u0027 cy\u003d\u002744\u0027 stroke\u003d\u0027black\u0027 style\u003d\u0027stroke-width: 1px;\u0027 r\u003d\u002720\u0027 fill\u003d\u0027DodgerBlue\u0027/\u003e\u003ccircle cx\u003d\u002754\u0027 cy\u003d\u002724\u0027 stroke\u003d\u0027black\u0027 style\u003d\u0027stroke-width: 1px;\u0027 r\u003d\u002715\u0027 fill\u003d\u0027crimson\u0027/\u003e\u003c/svg\u003e",
            "2017-01-23T20:42:34.373Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-pie-chart",
            "The Ultimate Pie Chart for Apache Zeppelin using \u003ca href src\u003d\"http://www.highcharts.com/\"\u003ehighcharts\u003c/a\u003e",
            "ultimate-pie-chart@0.0.2",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Line-Chart\" src\u003d\"data:image/png;base64,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\" \u003e",
            "2018-01-23T02:47:47.769Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-scatter-chart",
            "The Ultimate Scatter Chart for Apache Zeppelin using \u003ca href src\u003d\"http://www.highcharts.com/\"\u003ehighcharts\u003c/a\u003e",
            "ultimate-scatter-chart@0.0.2",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Heat-Map\" src\u003d\"data:image/png;base64,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\u003d\u003d\" \u003e",
            "2018-01-23T03:43:29.010Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-range-chart",
            "The Ultimate Range Chart for Apache Zeppelin using \u003ca href src\u003d\"http://www.highcharts.com/\"\u003ehighcharts\u003c/a\u003e",
            "ultimate-range-chart@0.0.2",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Line-Chart\" src\u003d\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAEaUlEQVRoQ+2af0xbVRTHz/vVH64gzFgt44eAmDBUhjYqmTHMKMlMiEuMGqZSulGCzMTNZNBtMRAWJsoSlkh0jHaAQ/zLsEVdzGJkLEM3Ya4zcTWDLZO2QzoLbmBL+9671zxcDaMIj9e+tJD2v6Y993w/53t773knJWCVvAiBo3JPSwvGcKm9aVfnSuWaBTGZW/oIgP4jTbvq4yBRrkDckSgbEJI+7shijmw9snsDTRL5ADgJk2A7tq25X24HI+5IubW2FWG8Q8kwXoamuWmfL5GmqG8R+Eo7jYf+kgsooiCGo7X7KSD3FhUUkA8m3zereXrGB6dtPyOv3/+9xdD44ooAKbPU4MK8RyFbt+4uvQLM8bNngEO4oKey2SYHTNARBwHAYYDrUpPwKr+GS53Qv1b0PChoJmSZEwNnkddJXKMnE5xScywW92+LYj5kx4DHCYDTUpPwSjaJTfvz3S3PPgcalTpkmS/P9CH/OPOVwpMoqyMRaVEM1lrvY5lZ6sezHr4L5I9JD3x3YQhIiszsNDZJdn1JRyLVa5m69272z3AnBZCslBRQ0DQ43G4YumLHaqWyrbW0/m2pji8VF9FTS0i2rdNcjHg4gTBSCe8pgvQrGOZAW9n+hqXEhPN5xEGCYso7zA8FWD5JrlNqPrRsIOFUV0psHERK1eSMiTsiZ3WFtWc7aIp4GWNcBJiwYQL6uys+Ov5/eWPSEWOXuYJnUXvimjX+dO0DlOf2rcCYx3MPQ9FtVuOBqoVgYg7kzrPMxfnN58TUbTg1+BMXQPyrCzkTcyBl7TX192o075cUbiTnV37oih1GXK4LR40f6Be8RyrNLRNAQLLc+17M+myqB/Jy02B+vybEBns21VVdyFLBAd0gwvArAER9QMdm3HwnRZf8yqYNT4SItY/+DpeGh2+Qw9o3Yv5mf9NSs4UE6H3p6UJYm5D4n94Ax8I3534AjucPthsad8c8iCBwR0/dFzOBwOvZulQiTauFyakpsI9eB5bnHRZDY/qKOLWCIk1d+6oBoNrPsnkKRjFKk2Tv4bcadq6oe0TMobAitlYcJFKPulIqGamYmLvZpYItC6S8Y2cSzynqKJIoRhivJylygEe85dj25qhfpKJBBAgS1OcZmspcn5HFJCdowHnTDSNOF0dQZIfV0FgptZqRiBMPYq1tVSoVVSXPbKTmThKFrvTk+R8BYbSp23RQ8oAvXBjRvRbKcX+en5OTkpueEZKzz3YRu254ehWj2o/DFSQ1/s7IdOnudyZ7DF54Ug/BKfvchL9cG4HLvzmBdqyVqiPsONFba3vXHlu2bl2+/pHckKRfnxuAW9N/N3RVfFgXtiKJC4gGMX22r4rn0SfF+qeIuV3p1TEXDF62c4jEOXLNdcWwiQYRFqvuqeuZ9vpKU+/XzrbYDvc4THm9nFLFvPfp1oao/T4EbcsCEQKE5wU1Q5cgjAsAE6d4QIej6UTQrWWDiLE5Gt+Jg0Sj6ovlXF2OrJq/OcXaNpGi5x99qVCZXaMquwAAAABJRU5ErkJggg\u003d\u003d\" \u003e",
            "2018-01-23T03:42:31.049Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "ultimate-column-chart-negative-values",
            "The Ultimate Column Chart for Apache Zeppelin using \u003ca href src\u003d\"http://www.highcharts.com/\"\u003ehighcharts\u003c/a\u003e",
            "ultimate-column-chart-negative-values@0.0.3",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003cimg width\u003d\"21\" height\u003d\"21\" class\u003d\"icon icons8-Line-Chart\" src\u003d\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAB1ElEQVRoQ+1azUrDQBCebFMPtWlJPejRd1AQvFihIPgMpVS0dw9SFC/tQZDiG7RgKX0JpWC9in2HHlXQHFJyaDcb2dAFtY2bkByWMntbMkP225nv29kfDVakaSuCA5QCUmnXG0DACzu5jLHnfu1uyO3VAtKpe0YmM9H1FJWBsSdOlrruTa/WaigJpLSzC1vmhgwHPI5e4OPLaiIQ6VTFMKh06h5GJMYEJu6KEVFRfpEjItHL7YsiIeQgSuL3TlvNKPYy20Q4wssDQshV3lh3ZD+k1NVtx8n2zlqJVgaJASnkjPPjvf28DMib9QmD0SsgkICZwoj8lF/OEUwtGalCfsfUwtRC+f2fLMgR5MgSjvhFp0aeQiqtb8Y8diiOc3hfidQSQPh+IkzjtZrSQMqlozA4oD94QCB8pgLPteLWWiK1MCKqkR0jghH5cxqPZJ+vGqha8xsn5UoUVC1ULVQtgGVHpqhaqFoB19O4squ4sqfTqeuCkdNle9XpjII1sX/djwiyb5qmzN3//m5ZS7e6ZtaAtbR0CGDZNkxndPHlgz8QIMVQo5gbiecTvFu9v9xmlFWj+BOddLsnt2Ph4z+qidAYsKGSj2oiYFgwTfQOMM5A4vp+A403Sm9lQCHKAAAAAElFTkSuQmCC\" \u003e",
            "2017-07-06T18:58:27.137Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "zeppelin-highmaps",
            "Zeppelin plugin to visualize data using Highmaps",
            "zeppelin-highmaps@1.0.0",
            "Apache-2.0",
            "\u003ci class\u003d\u0027fa fa-globe\u0027 style\u003d\u0027color: #ee8208;\u0027\u003e\u003c/i\u003e",
            "2018-01-30T15:46:40.265Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "zeppelin-highcharts-heatmap",
            "Heatmap Charts for Apache Zeppelin using highcharts.js",
            "zeppelin-highcharts-heatmap@0.0.5",
            "SEE LICENSE IN \u003cLICENSE\u003e",
            "\u003ci class\u003d\"fa fa-th\"\u003e\u003c/i\u003e",
            "2017-02-14T12:46:02.732Z",
            true));

    registries.add(new HeliumRegistry(
            "https://s3.amazonaws.com/helium-package/helium.json",
            "VISUALIZATION",
            "zeppelin-aggrid",
            "Data visualization with Ag-Grid for Apache Zeppelin",
            "zeppelin-aggrid@0.1.1",
            "MIT",
            "\u003ci class\u003d\u0027fa fa-table fa-rotate-270\u0027\u003e\u003c/i\u003e",
            "2018-02-19T21:47:14.621Z",
            true));
  }

}
