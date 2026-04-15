import { Routes } from '@angular/router';
import { MainLayoutComponent } from './components/layout/main-layout/main-layout.component';
import { authGuard } from './guards/auth.guard';

export const routes: Routes = [
    {
        path: 'login',
        loadComponent: () =>
            import('./pages/login/login.component').then((m) => m.LoginComponent),
    },
    {
        path: '',
        component: MainLayoutComponent,
        canActivate: [authGuard],
        children: [
            { path: '', redirectTo: 'dashboard', pathMatch: 'full' },
            {
                path: 'dashboard',
                loadComponent: () =>
                    import('./pages/dashboard/dashboard.component').then(
                        (m) => m.DashboardComponent
                    ),
                data: { title: 'Dashboard' },
            },
            {
                path: 'admin',
                loadComponent: () =>
                    import('./pages/admin/admin.component').then(
                        (m) => m.AdminComponent
                    ),
                data: { title: 'Admin Console', roles: ['admin'] },
            },
            {
                path: 'operator',
                loadComponent: () =>
                    import('./pages/operator/operator.component').then(
                        (m) => m.OperatorComponent
                    ),
                data: { title: 'Operator Controls', roles: ['admin', 'operator'] },
            },
            {
                path: 'meter/:id',
                loadComponent: () =>
                    import('./pages/meter-detail/meter-detail.component').then(
                        (m) => m.MeterDetailComponent
                    ),
                data: { title: 'Meter Detail' },
            },
            // Configuration routes (placeholder)
            {
                path: 'config/meters',
                loadComponent: () =>
                    import('./pages/config/meter-config/meter-config.component').then(
                        (m) => m.MeterConfigComponent
                    ),
                data: { title: 'Meter Management' },
            },
            // Alerts routes (placeholder)
            {
                path: 'alerts',
                loadComponent: () =>
                    import('./pages/alerts/alerts.component').then(
                        (m) => m.AlertsComponent
                    ),
                data: { title: 'Alerts' },
            },
            // Account
            {
                path: 'account',
                loadComponent: () =>
                    import('./pages/account/account.component').then(
                        (m) => m.AccountComponent
                    ),
                data: { title: 'Account' },
            },
            // Energy Reports
            {
                path: 'energy-report',
                loadComponent: () =>
                    import('./pages/energy-report/energy-report.component').then(
                        (m) => m.EnergyReportComponent
                    ),
                data: { title: 'Energy Reports' },
            },
        ],
    },
    { path: '**', redirectTo: '' },
];
