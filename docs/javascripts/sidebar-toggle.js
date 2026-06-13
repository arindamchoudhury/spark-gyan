(function () {
  function setup(sidebar, storageKey, collapseChar, expandChar) {
    var btn = document.createElement('button');
    btn.className = 'sidebar-toggle';
    btn.title = 'Toggle sidebar';

    var collapsed = localStorage.getItem(storageKey) === '1';
    if (collapsed) sidebar.classList.add('is-collapsed');
    btn.textContent = collapsed ? expandChar : collapseChar;

    btn.addEventListener('click', function () {
      var nowCollapsed = sidebar.classList.toggle('is-collapsed');
      localStorage.setItem(storageKey, nowCollapsed ? '1' : '0');
      btn.textContent = nowCollapsed ? expandChar : collapseChar;
    });

    sidebar.prepend(btn);
  }

  document.addEventListener('DOMContentLoaded', function () {
    var nav = document.querySelector('.md-sidebar--primary');
    var toc = document.querySelector('.md-sidebar--secondary');
    if (nav) setup(nav, 'sidebar-nav-collapsed', '◀', '▶');
    if (toc) setup(toc, 'sidebar-toc-collapsed', '▶', '◀');
  });
})();
