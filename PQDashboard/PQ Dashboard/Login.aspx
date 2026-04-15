<%@ Page Language="C#" AutoEventWireup="true" %>
<%@ Import Namespace="System.Data" %>
<%@ Import Namespace="System.Data.SqlClient" %>
<%@ Import Namespace="System.Web.Security" %>
<%@ Import Namespace="System.Configuration" %>

<script runat="server">
    protected string errorMessage = "";

    protected void Page_Load(object sender, EventArgs e)
    {
        if (User.Identity.IsAuthenticated) Response.Redirect("~/");

        if (IsPostBack)
        {
            string user = Request.Form["username"];
            string pass = Request.Form["password"];

            if (ValidateUser(user, pass))
            {
                FormsAuthentication.SetAuthCookie(user, false);
                string returnUrl = Request.QueryString["ReturnUrl"];
                if (string.IsNullOrEmpty(returnUrl))
                {
                    Response.Redirect("~/");
                }
                else
                {
                    Response.Redirect(returnUrl);
                }
            }
            else
            {
                errorMessage = "Invalid username or password!";
            }
        }
    }

    private bool ValidateUser(string username, string password)
    {
        bool isValid = false;
        GSF.Configuration.ConfigurationFile config = GSF.Configuration.ConfigurationFile.Current;
        GSF.Configuration.CategorizedSettingsElementCollection systemSettings = config.Settings["systemSettings"];
        string connString = systemSettings["ConnectionString"].Value;

        using (SqlConnection conn = new SqlConnection(connString))
        {
            // Kiểm tra bảng UserAccount (hoặc ApplicationUser tùy DB của bạn)
            string sql = "SELECT Password FROM UserAccount WHERE Name = @name";
            SqlCommand cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@name", username);

            try
            {
                conn.Open();
                object result = cmd.ExecuteScalar();

                if (result != null)
                {
                    string storedPassword = result.ToString();
                    if(GSF.Security.Cryptography.Cipher.GetPasswordHash(password) == storedPassword)
                    {
                        isValid = true;
                    }
                }
            }
            catch (Exception ex)
            {
                errorMessage = "An error occurred during user authentication.";
                return false;
            }
        }
        return isValid;
    }
</script>

<!DOCTYPE html>
<html>
<head runat="server">
    <title>Login - ATDigital Meter</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
        :root {
            --brand-blue: #2f4da2;
            --brand-blue-light: #6ea6f3;
            --text-main: #2f3a4a;
            --text-muted: #6f7785;
            --card-bg: rgba(255, 255, 255, 0.9);
            --input-border: #b8c2d6;
            --shadow-soft: 0 18px 45px rgba(21, 35, 78, 0.18);
        }

        html, body {
            width: 100%;
            height: 100%;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
            color: var(--text-main);
            background: radial-gradient(circle at 20% 15%, #eaf2ff 0%, #d7e2f6 35%, #ced8eb 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: auto;
        }

        .login-shell {
            width: 100%;
            max-width: 420px;
            padding: 28px 18px;
            box-sizing: border-box;
        }

        .login-card {
            background: var(--card-bg);
            border: 1px solid rgba(255, 255, 255, 0.75);
            border-radius: 22px;
            padding: 28px 28px 26px;
            box-shadow: var(--shadow-soft);
            backdrop-filter: blur(2px);
        }

        .brand {
            text-align: center;
            margin-bottom: 18px;
        }

        .brand img {
            display: inline-block;
            width: 250px;
            max-width: 100%;
            height: auto;
        }

        .field {
            margin: 0 0 14px;
        }

        .label {
            display: inline-block;
            margin: 0 0 6px 2px;
            font-size: 12px;
            color: var(--text-muted);
            font-weight: 600;
            letter-spacing: 0.2px;
        }

        .text-input {
            width: 100%;
            height: 44px;
            border: 1px solid var(--input-border);
            border-radius: 8px;
            padding: 0 12px;
            box-sizing: border-box;
            font-size: 14px;
            color: var(--text-main);
            outline: none;
            background: rgba(255, 255, 255, 0.92);
            transition: border-color .18s ease, box-shadow .18s ease;
        }

        .text-input:focus {
            border-color: var(--brand-blue-light);
            box-shadow: 0 0 0 3px rgba(110, 166, 243, 0.2);
        }

        .password-wrap {
            position: relative;
        }

        .password-wrap .text-input {
            padding-right: 44px;
        }

        .toggle-pass {
            position: absolute;
            right: 10px;
            top: 50%;
            transform: translateY(-50%);
            border: 0;
            background: transparent;
            color: #5a6477;
            font-size: 18px;
            line-height: 1;
            cursor: pointer;
            padding: 2px;
        }

        .toggle-pass:focus {
            outline: none;
            color: var(--brand-blue);
        }

        .btn-login {
            width: 100%;
            height: 44px;
            border: 0;
            border-radius: 11px;
            background: linear-gradient(90deg, #5b74b6 0%, #5fa0ef 100%);
            color: #ffffff;
            font-size: 17px;
            font-weight: 700;
            letter-spacing: 0.2px;
            cursor: pointer;
            box-shadow: 0 8px 18px rgba(69, 107, 186, 0.35);
            margin-top: 6px;
            transition: transform .15s ease, box-shadow .15s ease, filter .15s ease;
        }

        .btn-login:hover {
            filter: brightness(1.02);
        }

        .btn-login:active {
            transform: translateY(1px);
            box-shadow: 0 6px 14px rgba(69, 107, 186, 0.32);
        }

        .error {
            color: #9f2f37;
            background: rgba(255, 235, 238, 0.95);
            border: 1px solid #f4bcc2;
            border-radius: 9px;
            padding: 9px 11px;
            font-size: 13px;
            margin-bottom: 12px;
        }

        @media (max-width: 520px) {
            .login-shell {
                padding: 14px;
            }

            .login-card {
                padding: 22px 16px 18px;
                border-radius: 16px;
            }

            .brand img {
                width: 220px;
            }
        }
    </style>
</head>
<body>
    <div class="login-shell">
        <div class="login-card">
            <div class="brand">
                <img src="<%= ResolveUrl("~/Images/ATDigital_Meter.png") %>" alt="ATDigital Meter" />
            </div>

            <% if (!string.IsNullOrEmpty(errorMessage)) { %>
                <div class="error"><%= errorMessage %></div>
            <% } %>

            <form id="loginForm" runat="server" autocomplete="on">
                <div class="field">
                    <label class="label" for="username">Username*</label>
                    <input id="username" class="text-input" type="text" name="username" required />
                </div>

                <div class="field">
                    <label class="label" for="password">Password*</label>
                    <div class="password-wrap">
                        <input id="password" class="text-input" type="password" name="password" required />
                        <button type="button" id="togglePass" class="toggle-pass" aria-label="Show or hide password">&#128065;</button>
                    </div>
                </div>

                <button type="submit" class="btn-login">Log In</button>
            </form>
        </div>
    </div>

    <script>
        (function () {
            var toggle = document.getElementById('togglePass');
            var pass = document.getElementById('password');
            if (!toggle || !pass) return;

            toggle.addEventListener('click', function () {
                pass.type = pass.type === 'password' ? 'text' : 'password';
            });
        })();
    </script>
</body>
</html>
