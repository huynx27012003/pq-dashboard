import { inject } from '@angular/core';
import { Router, CanActivateFn } from '@angular/router';
import { AuthService } from '../services/auth.service';

export const authGuard: CanActivateFn = (route, state) => {
    const authService = inject(AuthService);
    const router = inject(Router);

    if (authService.isLoggedIn) {
        // Check for required roles on route data
        const requiredRoles = route.data?.['roles'] as string[];
        
        if (requiredRoles && requiredRoles.length > 0) {
           const userRole = authService.currentUser?.role;
           if (!userRole || !requiredRoles.includes(userRole)) {
               // User doesn't have the required role, redirect to dashboard
               router.navigate(['/']);
               return false;
           }
        }
        
        return true;
    }

    router.navigate(['/login'], { queryParams: { returnUrl: state.url } });
    return false;
};
