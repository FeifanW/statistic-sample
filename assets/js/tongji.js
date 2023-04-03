import "./jquery"
$(document).ready(function (){
    // 上报用户信息，以及访问数据到打点服务器
    $.get("http://localhost:8888",{
        "time":gettime(),
        "url":geturl(),
        "refer":getrefer(),
        "ua":getuser_agent(),
    })
})


function gettime(){
    var nowDate = new Date();
    return nowDate.toLocaleString();
}
function geturl(){
    return window.location.href;
}
function getrefer(){
    return document.referrer;
}
function getcookie(){
    return document.cookie;
}
function getuser_agent(){
    return navigator.userAgent;
}