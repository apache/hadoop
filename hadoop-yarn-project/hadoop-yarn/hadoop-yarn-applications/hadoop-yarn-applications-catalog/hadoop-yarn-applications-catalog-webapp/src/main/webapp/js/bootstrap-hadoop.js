/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

$(document).ready(function () {
  var $accordionToggler = $(this).find('[data-toggle="collapseAccordion"]');
  $accordionToggler.off('click').on('click', function (event) {
    var $this = $(this);
    $this.siblings('.panel-body').slideToggle(500);
    $this.children().children('.panel-toggle').toggleClass('fa-angle-down fa-angle-up');
    event.stopPropagation();
    return false;
  });
});
'use strict';

(function ($) {

  /**
   * jQuery plugin for navigation bars
   * Usage:
   * <pre>
   *   $('.navigation-bar').navigationBar();
   * </pre>
   *
   * @param {object} options see <code>$.fn.navigationBar.defaults</code>
   * @returns {$}
   */

  $.fn.navigationBar = function (options) {

    var settings = $.extend({}, $.fn.navigationBar.defaults, options);

    return this.each(function () {
      var _this = this;

      var containerSelector = '.navigation-bar-container';
      var $navigationContainer = $(this).find(containerSelector);
      var $sideNavToggler = $(this).find('[data-toggle=' + settings.navBarToggleDataAttr + ']');
      var $subMenuToggler = $(this).find('[data-toggle=' + settings.subMenuNavToggleDataAttr + ']');
      var firstLvlMenuItemsSelector = '.side-nav-menu>li';
      var secondLvlMenuItemsSelector = '.side-nav-menu>li>ul>li';
      var $moreActions = $(this).find('.more-actions');
      var $dropdownMenu = $moreActions.children('.dropdown-menu');

      $subMenuToggler.each(function (index, toggler) {
        return $(toggler).parent().addClass('has-sub-menu');
      });

      if (settings.fitHeight) {
        $(this).addClass('navigation-bar-fit-height');

        // make scrolling effect on side nav ONLY, i.e. not effected on ambari main contents
        $(this).find('.side-nav-menu').on('DOMMouseScroll mousewheel', function (ev) {
          var $this = $(this),
              scrollTop = this.scrollTop,
              scrollHeight = this.scrollHeight,
              height = $this.innerHeight(),
              delta = ev.originalEvent.wheelDelta,
              up = delta > 0;
          var prevent = function prevent() {
            ev.stopPropagation();
            ev.preventDefault();
            ev.returnValue = false;
            return false;
          };

          if (!up && -delta > scrollHeight - height - scrollTop) {
            // Scrolling down, but this will take us past the bottom.
            $this.scrollTop(scrollHeight);
            return prevent();
          } else if (up && delta > scrollTop) {
            // Scrolling up, but this will take us past the top.
            $this.scrollTop(0);
            return prevent();
          }
        });
      }

      //set main content left margin based on the width of side-nav
      var containerWidth = $navigationContainer.width();
      if (settings.moveLeftContent) {
        $(settings.content).css('margin-left', containerWidth);
      }
      if (settings.moveLeftFooter) {
        $(settings.footer).css('margin-left', containerWidth);
      }

      function popStateHandler() {
        var path = window.location.pathname + window.location.hash;
        $navigationContainer.find('li a').each(function (index, link) {
          var $link = $(link);
          var href = $link.attr('data-href') || $link.attr('href');
          if (path.indexOf(href) !== -1 && ['', '#'].indexOf(href) === -1) {
            $link.parent().addClass('active');
          } else {
            $link.parent().removeClass('active');
          }
        });
      }

      if (settings.handlePopState) {
        popStateHandler();
        $(window).bind('popstate', popStateHandler);
      }

      function clickHandler(el) {
        var $li = $(el).parent();
        var activeClass = settings.activeClass;

        var activeMenuItems = firstLvlMenuItemsSelector + '.' + activeClass;
        var activeSubMenuItems = secondLvlMenuItemsSelector + '.' + activeClass;
        $navigationContainer.find(activeMenuItems).removeClass(activeClass);
        $navigationContainer.find(activeSubMenuItems).removeClass(activeClass);
        $li.addClass(activeClass);
      }

      /**
       * Click on menu item
       */
      $(firstLvlMenuItemsSelector + '>a').on('click', function () {
        clickHandler(this);
      });

      /**
       * Click on sub menu item
       */
      $(secondLvlMenuItemsSelector + '>a').on('click', function () {
        clickHandler(this);
        $(this).parent().parent().parent().addClass(settings.activeClass);
      });

      /**
       * Slider for sub menu
       */
      $subMenuToggler.off('click').on('click', function (event) {
        // ignore click if navigation-bar is collapsed
        if ($navigationContainer.hasClass('collapsed')) {
          return false;
        }
        var $this = $(this);
        $this.siblings('.sub-menu').slideToggle(600, function () {
          var $topMenuItem = $this.parent();
          var $subMenu = $topMenuItem.find('ul');
          return $subMenu.is(':visible') ? $topMenuItem.removeClass('collapsed') : $topMenuItem.addClass('collapsed');
        });
        $this.children('.toggle-icon').toggleClass(settings.menuLeftClass + ' ' + settings.menuDownClass);
        event.stopPropagation();
        return false;
      });

      /**
       * Hovering effects for "more actions icon": "..."
       */
      $(this).find('.mainmenu-li>a').hover(function () {
        var $moreIcon = $(this).siblings('.more-actions');
        if ($moreIcon.length && !$navigationContainer.hasClass('collapsed')) {
          $moreIcon.css('display', 'inline-block');
        }
      }, function () {
        var $moreIcon = $(this).siblings('.more-actions');
        if ($moreIcon.length && !$navigationContainer.hasClass('collapsed')) {
          $moreIcon.hide();
        }
      });
      $moreActions.hover(function () {
        $(this).css('display', 'inline-block');
      });
      if (settings.fitHeight) {
        $moreActions.on('click', function () {
          // set actions submenu position
          var $moreIcon = $(this);
          var $header = $('.side-nav-header');
          $dropdownMenu.css({
            top: $moreIcon.offset().top - $header.offset().top + 20 + 'px',
            left: $moreIcon.offset().left + 'px'
          });
        });
      }
      $dropdownMenu.on('click', function () {
        // some action was triggered, should hide this icon
        var moreIcon = $(this).parent();
        setTimeout(function () {
          moreIcon.hide();
        }, 1000);
      });
      $navigationContainer.children('.side-nav-menu').scroll(function () {
        $moreActions.removeClass('open');
      });

      /**
       * Expand/collapse navigation bar
       */
      $sideNavToggler.click(function () {

        $navigationContainer.toggleClass('collapsed').promise().done(function () {
          var subMenuSelector = 'ul.sub-menu';
          var $subMenus = $navigationContainer.find(subMenuSelector);
          var $subMenuItems = $navigationContainer.find('.side-nav-menu>li');
          if ($navigationContainer.hasClass('collapsed')) {
            // set sub menu invisible when collapsed
            $subMenus.hide();
            $moreActions.hide();
            // set the hover effect when collapsed, should show sub-menu on hovering
            $subMenuItems.hover(function () {
              $(this).find(subMenuSelector).show();
              // set sub-menu position
              var $parent = $(this);
              var $header = $('.side-nav-header');
              if (settings.fitHeight) {
                $(this).find(subMenuSelector).css({
                  position: 'fixed',
                  top: $parent.offset().top - $header.offset().top + 'px',
                  left: 50 + 'px'
                });
              }
            }, function () {
              $(this).find(subMenuSelector).hide();
            });
          } else {
            // keep showing all sub menu
            $subMenus.show().each(function (index, item) {
              return $(item).parent().removeClass('collapsed');
            });
            $subMenuItems.unbind('mouseenter mouseleave');
            $navigationContainer.find('.toggle-icon').removeClass(settings.menuLeftClass).addClass(settings.menuDownClass);
            // set sub-menu position
            if (settings.fitHeight) {
              $(_this).find(subMenuSelector).css({
                position: 'relative',
                top: 0,
                left: 0
              });
            }
          }

          $navigationContainer.on('transitionend', function () {
            //set main content left margin based on the width of side-nav
            var containerWidth = $navigationContainer.width();
            if (settings.moveLeftContent) {
              $(settings.content).css('margin-left', containerWidth);
            }
            if (settings.moveLeftFooter) {
              $(settings.footer).css('margin-left', containerWidth);
            }
          });
          $sideNavToggler.find('span').toggleClass(settings.collapseNavBarClass + ' ' + settings.expandNavBarClass);
        });
        return false;
      });
    });
  };

  $.fn.navigationBar.defaults = {
    handlePopState: true,
    fitHeight: false,
    content: '#main',
    footer: 'footer',
    moveLeftContent: true,
    moveLeftFooter: true,
    menuLeftClass: 'glyphicon-menu-right',
    menuDownClass: 'glyphicon-menu-down',
    collapseNavBarClass: 'fa-angle-double-left',
    expandNavBarClass: 'fa-angle-double-right',
    activeClass: 'active',
    navBarToggleDataAttr: 'collapse-side-nav',
    subMenuNavToggleDataAttr: 'collapse-sub-menu'
  };
})(jQuery);
