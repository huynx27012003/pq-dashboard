using System.Web.Mvc;

namespace PQDashboard.Controllers
{
    public class TariffController : Controller
    {
        [HttpGet]
        public ActionResult Overview(string page, string meter)
        {
            ViewBag.Page = page;
            ViewBag.Meter = meter;
            return View("~/Views/Main/TariffOverview.cshtml");
        }
    }
}
