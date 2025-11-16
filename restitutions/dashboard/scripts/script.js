const grayImg64="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAgAAAAIABAAAAAAU42YnAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAAAmJLR0QADzoyPqMAAAAHdElNRQfoBhEHMQcMUHKBAAACVklEQVR42u3QAQ0AAAwCIOMb2z3HIQLpcxEgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAQIECBAgQIAAAWfiHg/xUpUmNwAAACV0RVh0ZGF0ZTpjcmVhdGUAMjAyNC0wNi0xN1QwNzo0OTowNiswMDowMOXRpv0AAAAldEVYdGRhdGU6bW9kaWZ5ADIwMjQtMDYtMTdUMDc6NDk6MDYrMDA6MDCUjB5BAAAAAElFTkSuQmCC";
function getServerUrl(){
    return `${location.protocol}//${location.hostname}${(location.port !== "")?':'+location.port:''}`;
}

function getDashboardUrl(){
    const baseUrl = getServerUrl();
    const webAppPath = Dashboards.getWebAppPath();
    return  `${baseUrl}${webAppPath}/api/repos/`;
}

document.addEventListener("DOMContentLoaded", function(e){
    const nav_els = document.getElementsByClassName("menu-item");
    Array.from(nav_els).forEach((nav) => nav.addEventListener('click', event => {
      if(nav.getAttribute("external_link")=="true") {
          window.top.location= nav.getElementsByTagName("a")[0].href;
      }
      else {
       const page = nav.getAttribute("page");
      if(page) {
            window.top.location = `${getDashboardUrl()}${encodeURIComponent(":public:dashboard:")}${page}.wcdf/generatedContent`;
        }
      }
    }));

    document.querySelectorAll('.menu-link').forEach(link => {
      link.addEventListener('click', event => {
        event.preventDefault();
      });
    });

    const pageParam = Dashboards.getParam("page");
    const navs = document.getElementsByClassName("menu-item");
    Array.from(navs).forEach((nav) => {
      if(nav.getAttribute('page') == pageParam) {
          nav.classList.add("active");
      }
    });
});