import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { MenuItem } from '../interfaces/menu-item.interface';
import { AuthService } from './auth.service';

@Injectable({
    providedIn: 'root',
})
export class SidebarService {
    private isMobileSubject = new BehaviorSubject<boolean>(false);
    private isCollapsedSubject = new BehaviorSubject<boolean>(false);

    public isMobile$ = this.isMobileSubject.asObservable();
    public isCollapsed$ = this.isCollapsedSubject.asObservable();

    constructor(private authService: AuthService) {
        this.checkScreenSize();
        if (typeof window !== 'undefined') {
            window.addEventListener('resize', () => this.checkScreenSize());
        }
    }

    private checkScreenSize(): void {
        if (typeof window !== 'undefined') {
            this.isMobileSubject.next(window.innerWidth < 768);
        }
    }

    public setIsMobile(isMobile: boolean): void {
        this.isMobileSubject.next(isMobile);
    }

    public toggleCollapsed(): void {
        this.isCollapsedSubject.next(!this.isCollapsedSubject.value);
    }

    public setCollapsed(collapsed: boolean): void {
        this.isCollapsedSubject.next(collapsed);
    }

    /**
     * Get menu items for sidebar
     */
    getMenuItems(): MenuItem[] {
        const userRole = this.authService.currentUser?.role || 'viewer';

        const allItems: MenuItem[] = [
            {
                label: 'Dashboard',
                icon: 'dashboard',
                route: '/dashboard',
            },
            {
                label: 'Energy Reports',
                icon: 'assessment',
                route: '/energy-report',
            },
            {
                label: 'Operator Controls',
                icon: 'engineering',
                route: '/operator',
                roles: ['admin', 'operator'],
            },
            {
                label: 'Admin Console',
                icon: 'security',
                route: '/admin',
                roles: ['admin'],
            },
            {
                label: 'Configuration',
                icon: 'settings',
                children: [
                    {
                        label: 'Meter Management',
                        icon: 'electric_meter',
                        route: '/config/meters',
                    },
                ],
            },
            {
                label: 'Alerts',
                icon: 'notifications',
                route: '/alerts',
            },
        ];

        return allItems.filter(item => !item.roles || item.roles.includes(userRole));
    }
}
