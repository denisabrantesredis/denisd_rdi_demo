(function ($) {
  $(".toggle-nav").click(function () {
    $("#sidebar-links .nav-menu").css("left", "0px");
  });
  $(".page-wrapper").attr(
    "class",
    "page-wrapper " + localStorage.getItem("page-wrapper")
  );
  if (localStorage.getItem("page-wrapper") === null) {
    $(".page-wrapper").addClass("compact-wrapper");
  }

  // left sidebar and vertical menu
  if ($("#pageWrapper").hasClass("compact-wrapper")) {
    $(".sidebar-title").append(
      '<div class="according-menu"><i class="fa fa-angle-right"></i></div>'
    );
    $(".sidebar-submenu, .menu-content").hide();
    $(".submenu-title").append(
      '<div class="according-menu"><i class="fa fa-angle-right"></i></div>'
    );
    $(".submenu-content").hide();
  }
  // toggle sidebar
  $nav = $(".sidebar-wrapper");
  $header = $(".page-header");
  $toggle_nav_top = $(".toggle-sidebar");
  $toggle_nav_top.click(function () {
    $nav.toggleClass("close_icon");
    $header.toggleClass("close_icon");
    $(window).trigger("overlay");
  });

  $(window).on("overlay", function () {
    $bgOverlay = $(".bg-overlay");
    $isHidden = $nav.hasClass("close_icon");
    if ($(window).width() <= 991 && !$isHidden && $bgOverlay.length === 0) {
      $('<div class="bg-overlay active"></div>').appendTo($("body"));
    }

    if ($isHidden && $bgOverlay.length > 0) {
      $bgOverlay.remove();
    }
  });

  $("body").on("click", ".bg-overlay", function () {
    $header.addClass("close_icon");
    $nav.addClass("close_icon");
    $(this).remove();
  });


  // page active
  if ($("#pageWrapper").hasClass("compact-wrapper")) {
    $(".sidebar-wrapper nav").find("a").removeClass("active");
    //$(".sidebar-wrapper nav").find("div").removeClass("active");

    var current = window.location.pathname;
    $(".sidebar-wrapper nav ul li a").filter(function () {
      var link = $(this).attr("href");
      if (link) {
        if (current.indexOf(link) != -1) {
          $(this).parents().children("a").addClass("active");
          $(this).parents().parents().children("ul").css("display", "block");
          $(this).addClass("active");
          $(this)
            .parent()
            .parent()
            .parent()
            .children("a")
            .find("div")
            .replaceWith(
              '<div class="according-menu"><i class="fa fa-angle-down"></i></div>'
            );
          $(this)
            .parent()
            .parent()
            .parent()
            .parent()
            .parent()
            .children("a")
            .find("div")
            .replaceWith(
              '<div class="according-menu"><i class="fa fa-angle-down"></i></div>'
            );
          return false;
        }
      }
    });
  }

  // active link
  if (
    $(".simplebar-wrapper .simplebar-content-wrapper") &&
    $("#pageWrapper").hasClass("compact-wrapper")
  ) {
    $(".simplebar-wrapper .simplebar-content-wrapper").animate(
      {
        scrollTop:
          $(".simplebar-wrapper .simplebar-content-wrapper a.active").offset()
            .top - 400,
      },
      1000
    );
  }
})($);
