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
    
    
    const mobileLinks = document.querySelectorAll('#mobile-menu-panel a');
    mobileLinks.forEach(link => {
        link.addEventListener('click', event => {
          event.preventDefault();
        
          const page = link.getAttribute('data-page');
          const href = link.getAttribute('href');
        
          // Lien interne (dashboard)
          if (page) {
            window.top.location =
              `${getDashboardUrl()}${encodeURIComponent(":public:dashboard:")}${page}.wcdf/generatedContent`;
          }
          // Lien externe
          else if (href && href !== '') {
            window.top.location = href;
          }
        
          // Ferme le menu mobile après clic (optionnel mais recommandé)
          const panel = document.getElementById('mobile-menu-panel');
          if (panel) {
            panel.style.display = 'none';
          }
        });
    });
    document.querySelectorAll('.menu-link, #mobile-menu-panel a').forEach(link => {
        link.addEventListener('click', event => {
        const page = link.getAttribute('page') || link.getAttribute('data-page');
        if (page) {
            event.preventDefault();
        }
        });
    });

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
    

  const toggleBtn = document.getElementById('mobile-menu-toggle');
  const panel = document.getElementById('mobile-menu-panel');

  toggleBtn.addEventListener('click', () => {
    panel.style.display = panel.style.display === 'flex' ? 'none' : 'flex';
  });

});