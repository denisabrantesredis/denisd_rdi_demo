(function ($) {
  if (localStorage.getItem("color"))
    $("#color").attr(
      "href",
      "../assets/css/" + localStorage.getItem("color") + ".css"
    );
  if (localStorage.getItem("dark")) $("body").attr("class", "dark-only");
  $(
    '<div class="customizer-links"><div class="nav flex-column nac-pills chabot-panel-transparent" id="c-pills-tab" role="tablist" aria-orientation="vertical"><a class="nav-link" id="c-pills-home-tab" data-bs-toggle="pill" href="#c-pills-home" role="tab" aria-controls="c-pills-home" aria-selected="true" data-original-title=""><div class="settings"><img class="img-fluid chatbot-icon" src="../assets/images/customizer/redis.png" alt="AI Assistant"></div><span>Online Assistant</span></a> </div></div><div class="customizer-contain"><div class="tab-content" id="c-pills-tabContent"><div class="customizer-header"><i class="icofont-close icon-close"></i><span class="f-20 f-w-600 txt-dark">Online Assistant</span><p class="mb-0">Get Help In Real Time <i class="fa fa-thumbs-o-up txt-primary"></i></p></div><div class="customizer-body custom-scrollbar"><div class="tab-pane fade show active" id="c-pills-home" role="tabpanel" aria-labelledby="c-pills-home-tab"> <div class="msger"><div class="msger-chat" id="chat-body"></div><form class="msger-inputarea" id="formchat" name="formchat"><div class="dropdown-form dropdown-toggle" role="main" data-bs-toggle="dropdown" aria-expanded="false"><i class="icon-plus"></i><div class="chat-icon dropdown-menu dropdown-menu-start"><div class="dropdown-item mb-2"><svg> <use href="../assets/svg/icon-sprite.svg#camera"></use></svg></div><div class="dropdown-item"><svg> <use href="../assets/svg/icon-sprite.svg#attchment"></use></svg></div></div></div><input class="msger-input two uk-textarea" type="text" id="chatinput" name="chatinput" placeholder="Type Message here.."><div class="open-emoji"><div class="second-btn uk-button"></div></div><button class="msger-send-btn" type="button" onclick="sendmessage();"><i class="fa fa-location-arrow"></i></button></form></div></div>'
  ).appendTo($("body"));  
  (function () {})();
  //live customizer js  
  $(document).ready(function () { 
    $(".customizer-color li").on("click", function () {
      $(".customizer-color li").removeClass("active");
      $(this).addClass("active");
      var color = $(this).attr("data-attr");
      var primary = $(this).attr("data-primary");
      var secondary = $(this).attr("data-secondary");
      localStorage.setItem("color", color);
      localStorage.setItem("primary", primary);
      localStorage.setItem("secondary", secondary);
      localStorage.removeItem("dark");
      $("#color").attr("href", "../assets/css/" + color + ".css");
      $(".dark-only").removeClass("dark-only");
      //location.reload(true);
    });

    $(".customizer-color.dark li").on("click", function () {
      $(".customizer-color.dark li").removeClass("active");
      $(this).addClass("active");
      $("body").attr("class", "dark-only");
      localStorage.setItem("dark", "dark-only");
    });

    if (localStorage.getItem("primary") != null) {
      document.documentElement.style.setProperty(
        "--theme-deafult",
        localStorage.getItem("primary")
      );
    }
    if (localStorage.getItem("secondary") != null) {
      document.documentElement.style.setProperty(
        "--theme-secondary",
        localStorage.getItem("secondary")
      );
    }

    // Capture 'enter' input in chatbot textarea
    const inputField = document.getElementById("chatinput");
    inputField.addEventListener("keydown", function(event) {
      // console.log("Pressed key: " + event.key);
      if (event.key === "Enter") {
        sendmessage();
      }
    });

    const form = document.getElementById("formchat");
      form.addEventListener('submit', (event) => {
        console.log("Intercepting page reload!");
        event.preventDefault();
      // Your code to handle form submission goes here
    });



    // Customizer (the Chatbot panel)
    // function to detect class changes to the customizer panel (meaning, opening or closing the chat window)

    $.fn.onClassChange = function(cb) {
      return $(this).each((_, el) => {
        new MutationObserver(mutations => {
          mutations.forEach(mutation => cb && cb(mutation.target, mutation.target.className));
        }).observe(el, {
          attributes: true,
          attributeFilter: ['class'] // only listen for class attribute changes 
        });
      });
    }

    const $chatbody = $("#chat-body").onClassChange((el, newClass) => console.log(`#${el.id} had its class updated to: ${newClass}`));

    // Load Chatbot UI
    $(
      ".customizer-links #c-pills-home-tab, .customizer-links #c-pills-layouts-tab"
    ).click(function () {
      var hostname = "localhost";
      var port = "5000"

      // Show the Panel
      $(".customizer-contain").addClass("open");
      $(".customizer-links").addClass("open");         

      // Show Loading gif
      var htmlresponse = "";
      htmlresponse += "<div style='text-align:center;margin-top: 250px;'> \n";
      htmlresponse += "  <img src='../assets/images/gif/loading2.gif' style='width: 80px; height: 80px;'> \n";
      htmlresponse += "</div> \n";
      $("#chat-body").html(htmlresponse);

      console.log("--> Loading Chatbot Panel")
        var serverurl = "http://" + hostname + ":" + port + "/chatbot"

        let request = new XMLHttpRequest();
        request.open("GET", serverurl);
        request.setRequestHeader("Content-type", "application/json");
        request.send();
        request.onload = () => {
            var responseStatus = request.status;
            console.log("--> Response status: " + responseStatus);
            if (responseStatus == 200) { 
              var hresponse = JSON.parse(request.response);
              // console.log("--> Bot Greeting: " + JSON.stringify(hresponse))
              var final_html = "";
              for (let i = 0; i < hresponse.length; i++) {
                var html = hresponse[i]['html']
                final_html += html;
                final_html += "\n";
              }
              // console.log("--> Bot HTML: " + final_html);
              $("#chat-body").html(final_html);
            }
        }

    });

    $(".chat-popup").click(function () {
      $(".customizer-contain").addClass("open");
      $(".customizer-links").addClass("open");
    });

    $(".close-customizer-btn").on("click", function () {
      $(".floated-customizer-panel").removeClass("active");
    });

    $(".customizer-contain .icon-close").on("click", function () {
      $(".customizer-contain").removeClass("open");
      $(".customizer-links").removeClass("open");

    });

    $(".color-apply-btn").click(function () {
      location.reload(true);
    });

    // var primary = document.getElementById("ColorPicker1").value;
    // document.getElementById("ColorPicker1").onchange = function () {
    //   primary = this.value;
    //   localStorage.setItem("primary", primary);
    //   document.documentElement.style.setProperty("--theme-primary", primary);
    // };

    // var secondary = document.getElementById("ColorPicker2").value;
    // document.getElementById("ColorPicker2").onchange = function () {
    //   secondary = this.value;
    //   localStorage.setItem("secondary", secondary);
    //   document.documentElement.style.setProperty(
    //     "--theme-secondary",
    //     secondary
    //   );
    // };

    // $(".customizer-color.dark li").on("click", function () {
    //   $(".customizer-color.dark li").removeClass("active");
    //   $(this).addClass("active");
    //   $("body").attr("class", "dark-only");
    //   localStorage.setItem("dark", "dark-only");
    // });

    // $(".customizer-mix li").on("click", function () {
    //   $(".customizer-mix li").removeClass("active");
    //   $(this).addClass("active");
    //   var mixLayout = $(this).attr("data-attr");
    //   $("body").attr("class", mixLayout);
    // });

    // $(".sidebar-setting li").on("click", function () {
    //   $(".sidebar-setting li").removeClass("active");
    //   $(this).addClass("active");
    //   var sidebar = $(this).attr("data-attr");
    //   $(".sidebar-wrapper").attr("data-layout", sidebar);
    // });

    // $(".sidebar-main-bg-setting li").on("click", function () {
    //   $(".sidebar-main-bg-setting li").removeClass("active");
    //   $(this).addClass("active");
    //   var bg = $(this).attr("data-attr");
    //   $(".sidebar-wrapper").attr("class", "sidebar-wrapper " + bg);
    // });

    $(".sidebar-type li").on("click", function () {
      $("body").append("");
      console.log("test");
      var type = $(this).attr("data-attr");

      var boxed = "";
      if ($(".page-wrapper").hasClass("box-layout")) {
        boxed = "box-layout";
      }
      switch (type) {
        case "compact-sidebar": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-wrapper " + boxed
          );
          $(this).addClass("active");
          localStorage.setItem("page-wrapper", "compact-wrapper");
          break;
        }
        case "normal-sidebar": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper horizontal-wrapper " + boxed
          );
          $(".logo-wrapper")
            .find("img")
            .attr("src", "../assets/images/logo/logo.png");
          localStorage.setItem("page-wrapper", "horizontal-wrapper");
          break;
        }
        case "default-body": {
          $(".page-wrapper").attr("class", "page-wrapper  only-body" + boxed);
          localStorage.setItem("page-wrapper", "only-body");
          break;
        }
        case "dark-sidebar": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-wrapper dark-sidebar" + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "compact-wrapper dark-sidebar"
          );
          break;
        }
        case "compact-wrap": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-sidebar" + boxed
          );
          localStorage.setItem("page-wrapper", "compact-sidebar");
          break;
        }
        case "color-sidebar": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-wrapper color-sidebar" + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "compact-wrapper color-sidebar"
          );
          break;
        }
        case "compact-small": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-sidebar compact-small" + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "compact-sidebar compact-small"
          );
          break;
        }
        case "box-layout": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-wrapper box-layout " + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "compact-wrapper box-layout"
          );
          break;
        }
        case "enterprice-type": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper horizontal-wrapper enterprice-type" + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "horizontal-wrapper enterprice-type"
          );
          break;
        }
        case "modern-layout": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-wrapper modern-type" + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "compact-wrapper modern-type"
          );
          break;
        }
        case "material-layout": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper horizontal-wrapper material-type" + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "horizontal-wrapper material-type"
          );

          break;
        }
        case "material-icon": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-sidebar compact-small material-icon" + boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "compact-sidebar compact-small material-icon"
          );

          break;
        }
        case "advance-type": {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper horizontal-wrapper enterprice-type advance-layout" +
              boxed
          );
          localStorage.setItem(
            "page-wrapper",
            "horizontal-wrapper enterprice-type advance-layout"
          );

          break;
        }
        default: {
          $(".page-wrapper").attr(
            "class",
            "page-wrapper compact-wrapper " + boxed
          );
          localStorage.setItem("page-wrapper", "compact-wrapper");
          break;
        }
      }
      // $(this).addClass("active");
      location.reload(true);
    });

    $(".main-layout li").on("click", function () {
      $(".main-layout li").removeClass("active");
      $(this).addClass("active");
      var layout = $(this).attr("data-attr");
      $("body").attr("class", layout);
      $("html").attr("dir", layout);
    });

    $(".main-layout .box-layout").on("click", function () {
      $(".main-layout .box-layout").removeClass("active");
      $(this).addClass("active");
      var layout = $(this).attr("data-attr");
      $("body").attr("class", "box-layout");
      $("html").attr("dir", layout);
    });
  });
})(jQuery);
